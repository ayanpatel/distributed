package main

import (
	"fmt"
    "math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"
	"crypto/md5"
	"encoding/binary"
	"math"
	"sort"
)

const NumNodes = 4
//const NumClusters = 1
//const tokensPerCluster = 4
//const numPartitions = NumClusters * tokensPerCluster

const bufferSize = 256

// membership constants in ms
const hbInterval = 200
const advertInterval = 400
const failInterval = 4000

// election constants in ms
const fTimeoutMin = 150
const fTimeoutMax = 300
const electionDuration = 100
const leaderHBTimeout = 50

// maximum number of database commits a leader can make per election
const maxLeaderCommits = 3

// message types
const (
	Put         = iota
	Get         = iota
	Remove 		= iota
    LogUpdate   = iota
	Ack         = iota
	HB          = iota
	VoteRequest = iota
	Vote        = iota
	LeaderHB    = iota
	Down        = iota
	Up          = iota
	Kill        = iota
)

//log status
const (
    Success = iota
    Failure = iota
)

// node election states
const (
	follower = iota
	candidate = iota
	leader = iota
)

var NumClusters int
var TokensPerCluster int

type Node struct {
	controlChannel     chan Message
	clientReqChannel   chan Message
	clientRespChannel  chan KeyValue
	nodeChannels       []chan Message
	nodeHBState		   HBState
	down               bool
	electionState      ElectionState
	dataStore          map[string] string
	tokens             TokenList
    clusterChannels    []chan Message
	clusterHBState     HBState
    logTable		   []LogEntry
    commitIndex        int
}

type LogEntry struct {
	term       int
	data       KeyValue
	Type       int
    replicas   int
}

type HBEntry struct {
	count       uint
	lastUpdated time.Time
	alive        bool
}

type HBState struct {
	id int
	hbTable  []HBEntry
	lastHBAdvert time.Time
	wakeupWatch map[int]struct{}
	wakeupCallback func(n* Node, i int)
}

type Message struct {
	src     	int
	mType   	int
	data    	KeyValue
	hbTable 	[]HBEntry
	term   		int
	logIndex	int
	logTerm 	int
	logEntries 	[]LogEntry
    commitIndex int
    logStatus   int
}

type ElectionState struct {
	nodeState       int
	term            int
	votedThisTerm   bool
	voteCount       int
	timeoutStart    time.Time
	timeoutDuration time.Duration
	currentLeader   int
}

type KeyValue struct {
	key    string
	value  string
}

type Token struct {
	id         uint64
    clusterId  int
}

type TokenList []Token

func (t TokenList) Len() int {
    return len(t)
}

func (t TokenList) Less(i, j int) bool {
    return t[i].id < t[j].id
}

func (t TokenList) Swap(i, j int) {
    t[i], t[j] = t[j], t[i]
}

func hashId(key string) uint64 {
	s := md5.Sum([]byte(key))
	h := binary.LittleEndian.Uint64(s[:])
	return h
}

func findNextPartitionPosition(tokens TokenList) int {
	var last uint64

	for i := 0; i < NumClusters && i < len(tokens); i++ {
		if i == 0 {
			last = tokens[i].id
		} else {
			if tokens[i].id - last > math.MaxUint64 / uint64(NumClusters * TokensPerCluster) {
				return i
			} else if i == NumClusters - 1 {
				panic("ring full")
			}
			last = tokens[i].id
		}
	}
	return 0
}

func createTokens(tokens TokenList, clusterId int) TokenList {
    partPos := findNextPartitionPosition(tokens)
    for i := 0; i < TokensPerCluster; i++ {
        var t Token
        t.id = uint64(i) * (math.MaxUint64 / uint64(TokensPerCluster)) +
            uint64(partPos) * (math.MaxUint64 / uint64(TokensPerCluster * NumClusters))
        t.clusterId = clusterId
        tokens = append(tokens, t)
    }
    sort.Sort(tokens)
    return tokens
}

func createClusters(wg *sync.WaitGroup) [][]Node {
	clusters := make([][]Node, NumClusters)
    clusterChans := make([]chan Message, NumClusters)
    tokens := []Token{}

    for c := 0; c < NumClusters; c++ {
        clusterChans[c] = make(chan Message, bufferSize)
        tokens = createTokens(tokens, c)
    }

    for c := 0; c < NumClusters; c++ {
        clusters[c] = createNodes(wg, tokens, clusterChans, c)
    }

    return clusters
}

func createNodes(wg *sync.WaitGroup, tokens TokenList, clusterChans []chan Message, clusterId int) []Node {
	// used to send messages between nodes
	nodeChans := make([]chan Message, NumNodes)
	// used to control status of nodes
	controlChans := make([]chan Message, NumNodes)
	// used to send data to nodes from imaginary clients
	clientReqChans := make([]chan Message, NumNodes)
	// used to send data to nodes from imaginary clients
	clientRespChans := make([]chan KeyValue, NumNodes)
	nodes := make([]Node, NumNodes)
    
	for i := 0; i < NumNodes; i++ {
		controlChans[i] = make(chan Message, bufferSize)
		nodeChans[i] = make(chan Message, bufferSize)
		clientReqChans[i] = make(chan Message, bufferSize)
		clientRespChans[i] = make(chan KeyValue, bufferSize)
	}

	for i := 0; i < NumNodes; i++ {
		nodes[i].initNode(i, controlChans[i], clientReqChans[i], clientRespChans[i], nodeChans,
            tokens, clusterChans, clusterId)
		wg.Add(1)
		go nodes[i].runNode(wg)
	}
	return nodes
}

func reviveNode(i int, controlChan chan Message) {
	var up Message
	up.mType = Up
	controlChan <- up
}

func downNode(i int, controlChan chan Message) {
	var down Message
	down.mType = Down
	controlChan <- down
}

func killAllNodes(nodes []Node) {
	for i := 0; i < len(nodes); i++ {
		var kill Message
		kill.mType = Kill
		nodes[i].controlChannel <- kill
	}
}

func killAllClusters(clusters [][]Node) {
	for i := 0; i < len(clusters); i++ {
		killAllNodes(clusters[i])
	}
}

func (s *HBState) initHBState(numSites int, id int) {
	s.hbTable = make([]HBEntry, numSites)
	s.hbTable[id].count = 1
	s.hbTable[id].lastUpdated = time.Now()
	s.hbTable[id].alive = true
	s.id = id
	s.wakeupWatch = make(map[int]struct{})
}

func (n *Node) initNode(id int, controlChan chan Message, clientReqChan chan Message,
	clientRespChan chan KeyValue, nodeChans []chan Message, 
    tokens TokenList, clusterChans []chan Message, clusterId int) {

	n.controlChannel = controlChan
	n.clientReqChannel = clientReqChan
	n.clientRespChannel = clientRespChan
	n.nodeChannels = nodeChans
	n.nodeHBState.initHBState(NumNodes, id)
	n.clusterHBState.initHBState(NumClusters, clusterId)
	n.clusterHBState.wakeupCallback = updateOwnerCluster
	n.down = false
	n.electionState.currentLeader = -1
	n.dataStore = make(map[string] string)
    n.tokens = tokens
    n.clusterChannels = clusterChans
    n.logTable = []LogEntry{}
    n.commitIndex = -1
}

func (n *Node) runNode(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		n.checkForControlMessage()

		if !n.down {
			n.checkForNodeMessage()
			n.checkForClientMessage()
            n.checkForClusterMessage()
			n.nodeHBState.handleMembership(n.nodeChannels, NumNodes)
			n.handleElection()
		}
	}
}

func (n *Node) checkForControlMessage() {
	// read for a down message from the control routine if its there
	select {
	case m := <-n.controlChannel:
		if m.mType == Down {
			n.down = true
			fmt.Println("cluster", n.clusterHBState.id, "node", n.nodeHBState.id, "downed")
		} else if m.mType == Up {
			n.down = false
			n.nodeHBState.initHBState(NumNodes, n.nodeHBState.id)
			n.clusterHBState.initHBState(NumClusters, n.clusterHBState.id)
			n.becomeFollower(n.electionState.term)
			fmt.Println("cluster", n.clusterHBState.id, "node", n.nodeHBState.id, "upped")
		} else if m.mType == Kill {
			fmt.Println("cluster", n.clusterHBState.id, "node", n.nodeHBState.id, "killed")
			runtime.Goexit()
		}
	default:
		break
	}
}

func (n *Node) checkForNodeMessage() {
	select {
	case m := <-n.nodeChannels[n.nodeHBState.id]:
		// election
		if m.term > n.electionState.term {
			n.becomeFollower(m.term)
		}
		if m.mType == LeaderHB {
			n.electionState.currentLeader = m.src
			n.clusterHBState.hbTable = m.hbTable
			n.becomeFollower(m.term)
		} else if m.mType == VoteRequest {
			if !n.electionState.votedThisTerm {
                if len(n.logTable) != 0 {
                    if n.logTable[len(n.logTable)-1].term > m.logTerm {
                        //Vote denied
                    } else if n.logTable[len(n.logTable)-1].term == m.logTerm {
                        if len(n.logTable)-1 > m.logIndex {
                            //Vote denied
                        } else {
                            n.sendVote(m.src)
                        }
                    } else {
                        n.sendVote(m.src)
                    }
                } else {
                    n.sendVote(m.src)
                }	
			}
		} else if m.mType == Vote {
			if n.electionState.nodeState == follower {
				//leftover vote from last election, ignore, all good
			} else if n.electionState.nodeState == candidate {
				if m.term == n.electionState.term {
					n.electionState.voteCount++
				}
			} else if n.electionState.nodeState == leader {
				//might be another vote, but we already won, all good
			}
		}

		// membership
		if m.mType == HB {
			n.nodeHBState.updateTable(m.hbTable, n)
		} else if m.mType == Put {
			if n.nodeHBState.id == n.electionState.currentLeader {
				n.handleRequest(m)
			} else {
				panic(strconv.Itoa(n.nodeHBState.id) + " is not the leader and recieved a put forward from node " +
					strconv.Itoa(m.src))
			}
		} else if m.mType == Remove {
			if n.nodeHBState.id == n.electionState.currentLeader {
				n.handleRequest(m)
			}
		} else if m.mType == LogUpdate {
			if m.src == n.electionState.currentLeader {
				var ack Message
				ack.src = n.nodeHBState.id
				ack.mType = Ack
                if len(n.logTable) == 0 && m.logIndex == 0 && m.logTerm == 0 {
                    n.logTable = append(n.logTable, m.logEntries...)
                    ack.logIndex = len(n.logTable) - 1
                    ack.logStatus = Success
                    n.handleCommits(m.commitIndex)
                } else if m.logIndex == len(n.logTable)-1 && m.logTerm == n.logTable[len(n.logTable)-1].term {
                    n.logTable = append(n.logTable, m.logEntries...)
                    ack.logIndex = len(n.logTable) - 1
                    ack.logStatus = Success
                    n.handleCommits(m.commitIndex)
                } else {
					ack.logIndex = len(n.logTable) - 1
					ack.logStatus = Failure
				}
				n.nodeChannels[n.electionState.currentLeader] <- ack
			}
		} else if m.mType == Ack {
			if n.nodeHBState.id == n.electionState.currentLeader {
				if m.logStatus == Success {
					n.logTable[m.logIndex].replicas += 1
					if n.logTable[m.logIndex].replicas > len(n.nodeHBState.hbTable)/2 {
						n.handleCommits(m.logIndex)
					}
				} else if m.logStatus == Failure {
					var lm Message
					lm.mType = LogUpdate
					if m.logIndex < 0 {
						lm.logIndex = 0
						lm.logTerm = n.logTable[0].term
						lm.logEntries = n.logTable[0:]
					} else {
						lm.logIndex = m.logIndex
						lm.logTerm = n.logTable[m.logIndex].term
						lm.logEntries = n.logTable[m.logIndex:]
					}
					lm.commitIndex = n.commitIndex
					n.nodeChannels[m.src] <- lm
				}
			}
		} else if m.mType == Get {
            if n.nodeHBState.id == n.electionState.currentLeader {
                n.clientRespChannel <- KeyValue{m.data.key, n.dataStore[m.data.key]}
            }
        }
	default:
		break
	}
}

func (n *Node) handleRequest(m Message) {
    var lm Message
    lm.mType = LogUpdate
    if len(n.logTable) != 0 {
        lm.logIndex = len(n.logTable) - 1
        lm.logTerm = n.logTable[len(n.logTable)-1].term
    } else {
        lm.logIndex = 0
        lm.logTerm = 0
    }
    log := LogEntry{n.electionState.term, m.data, m.mType, 1}
    n.logTable = append(n.logTable, log)
    lm.logEntries = []LogEntry{log}
    lm.commitIndex = n.commitIndex
    n.broadcastMessage(lm)
    fmt.Printf("node %v handled request for key %v, commit index is %v\n", n.nodeHBState.id, m.data.key,
    	lm.commitIndex)
}

func (n *Node) handleCommits(commitIndex int) {
    for i := n.commitIndex + 1; i <= commitIndex; i++ {
        if n.logTable[i].Type == Put {
            n.dataStore[n.logTable[i].data.key] = n.logTable[i].data.value
            fmt.Printf("node %v handled commit for key %v\n", n.nodeHBState.id, n.logTable[i].data.key)
        } else if n.logTable[i].Type == Remove {
            delete(n.dataStore, n.logTable[i].data.key)
        }
    }
    n.commitIndex = commitIndex
}

func (n *Node) checkForClientMessage() {
    var cluster int
	select {
	case m := <- n.clientReqChannel:
		if m.mType == Put {
            cluster = n.getCluster(m.data.key)
            if n.clusterHBState.id == cluster {
            	fmt.Println("cluster", n.clusterHBState.id, "received a put", m.data,
            		"which it is responsible for")
                if n.nodeHBState.id == n.electionState.currentLeader {
					n.handleRequest(m)
                } else if n.electionState.currentLeader == -1 {
                    // put it back until a leader has been decided
                    n.clientReqChannel <- m
                } else {
                    // forward to leader
                	m.src = n.nodeHBState.id
                    n.nodeChannels[n.electionState.currentLeader] <- m
                }
            } else {
            	// our cluster isn't reponsible for this data
				n.handleMessageForOtherCluster(m, cluster)
            }
		} else if m.mType == Get {
            cluster = n.getCluster(m.data.key)
            if n.clusterHBState.id == cluster {
                if n.nodeHBState.id == n.electionState.currentLeader {
                    n.clientRespChannel <- KeyValue{m.data.key, n.dataStore[m.data.key]}
                } else {
                    n.nodeChannels[n.electionState.currentLeader] <- m
                }
                
            } else {
                n.clusterChannels[cluster] <- m
            }
		}
	default:
		break
	}
}

func (n *Node) handleMessageForOtherCluster(m Message, cluster int) {
	if n.clusterHBState.hbTable[cluster].alive {
		fmt.Println("cluster", n.clusterHBState.id,
			"received data it is not responsible for and is " +
			"forwarding to a living cluster", m.data)
		m.src = n.clusterHBState.id
		n.clusterChannels[cluster] <- m
	} else {
		tokenIndex := n.getTokenIndex(m.data.key)

		for ; !n.clusterHBState.hbTable[n.tokens[tokenIndex].clusterId].alive ; {
			tokenIndex = (tokenIndex + 1) % len(n.tokens)
		}

		fmt.Println(n.clusterHBState.hbTable)

		if n.tokens[tokenIndex].clusterId == n.clusterHBState.id {
			fmt.Println("cluster", n.clusterHBState.id,
				"received data it is not usually responsible for " +
				"but the owner cluster is down, so it will store", m.data)
			n.handleRequest(m)
			n.clusterHBState.wakeupWatch[cluster] = struct{}{}
		} else {
			fmt.Println("cluster", n.clusterHBState.id,
				"received data it is not responsible for and is " +
				"forwarding to a cluster that is not usually responsible for this data, but owner " +
				"cluster is down", m.data)
			m.src = n.clusterHBState.id
			n.clusterChannels[n.tokens[tokenIndex].clusterId] <- m
		}
	}
}

func (n *Node) checkForClusterMessage() {
	if n.nodeHBState.id == n.electionState.currentLeader {
        select {
        case m := <- n.clusterChannels[n.clusterHBState.id]:
            if m.mType == Put {
            	cluster := n.getCluster(m.data.key)
            	if cluster == n.clusterHBState.id {
					n.handleRequest(m)
				} else {
					n.handleMessageForOtherCluster(m, cluster)
				}
			} else if m.mType == Get {
                n.clientRespChannel <- KeyValue{m.data.key, n.dataStore[m.data.key]}
            } else if m.mType == Remove {
				n.handleRequest(m)
            } else if m.mType == HB {
				n.clusterHBState.updateTable(m.hbTable, n)
			}
        default:
            break
        }
    }
}

func (n *Node) search(id string) int {
    searchFn := func(i int) bool {
        return n.tokens[i].id >= hashId(id)
    }
    return sort.Search(n.tokens.Len(), searchFn)
}

func (n *Node) getCluster(key string) int {
    i := n.search(key)
    if i >= n.tokens.Len() {
        i = 0
    }
    return n.tokens[i].clusterId
}

func (n *Node) getTokenIndex(key string) int {
	i := n.search(key)
	if i >= n.tokens.Len() {
		i = 0
	}
	return i
}

func (s *HBState) handleMembership(chans []chan Message, numSites int) {
	s.checkForDeaths(s.id)
	// update HB
	if time.Since(s.hbTable[s.id].lastUpdated) > time.Duration(hbInterval)*time.Millisecond {
		s.hbTable[s.id].count++
		s.hbTable[s.id].lastUpdated = time.Now()
	}
	// advertise heartbeat table to neighbors
	if time.Since(s.lastHBAdvert) > time.Duration(advertInterval)*time.Millisecond {
		advertise(rand.Intn(numSites), s.id, s.hbTable, chans)
		advertise(rand.Intn(numSites), s.id, s.hbTable, chans)
		s.lastHBAdvert = time.Now()
	}
}

func (s *HBState) checkForDeaths(id int) {
	for i := 0; i < len(s.hbTable); i++ {
		// if it is a valid entry
		if s.hbTable[i].count != 0 {
			// mark node as dead
			if time.Since(s.hbTable[i].lastUpdated) > time.Duration(failInterval)*
				time.Millisecond && s.hbTable[i].alive {

				s.hbTable[i].alive = false
				fmt.Println("node", id, "marked", i, "as dead")
			}
			// cleanup entry after waiting another failInterval
			if !s.hbTable[i].alive && time.Since(s.hbTable[i].lastUpdated) >
				time.Duration(2*failInterval)*time.Millisecond {

				s.hbTable[i].count = 0
				s.hbTable[i].lastUpdated = time.Time{}

				fmt.Println("node", id, "removed", i, "from table")
			}
		}
	}
}

func (s *HBState) updateTable(otherTable []HBEntry, n *Node) {
	for i := 0; i < len(s.hbTable); i++ {
		// nodes aren't supposed to advertise things that are dead
		if s.hbTable[i].count < otherTable[i].count && otherTable[i].alive {
			s.hbTable[i].count = otherTable[i].count
			s.hbTable[i].lastUpdated = time.Now()
			if s.hbTable[i].alive == false {
				s.hbTable[i].alive = true
				_, present := s.wakeupWatch[i]
				if present {
					s.wakeupCallback(n, i)
					delete(s.wakeupWatch, i)
				}
			}
		}
	}
}

func advertise(i int, id int, table []HBEntry, chans []chan Message) {
	var m Message
	m.mType = HB
	m.hbTable = table
	m.src = id
	// send the data, but don't block if buffer is full
	select {
	case chans[i] <- m:
	default:
		break
	}
}

func updateOwnerCluster(n *Node, i int) {
	fmt.Println("update owner cluster called")
	for key, value := range n.dataStore {
		if n.getCluster(key) == i {
			var m Message
			m.data.key = key
			m.data.value = value
			m.src = n.clusterHBState.id
			m.mType = Put
			n.clusterChannels[i] <- m

			m.mType = Remove
			n.clusterChannels[n.clusterHBState.id] <- m
		}
	}
}

func (n *Node) becomeFollower(term int) {
	n.updateTerm(term)
	n.electionState.nodeState = follower
	n.resetFollowerTimeout()
	//fmt.Println(n.id, "became follower at", n.electionState.term, time.Now())
}

func (n *Node) updateTerm(term int) {
	n.electionState.term = term
	n.electionState.votedThisTerm = false
}

func (n *Node) resetFollowerTimeout() {
	n.electionState.timeoutStart = time.Now()
	n.electionState.timeoutDuration =
		time.Duration(rand.Intn(fTimeoutMax-fTimeoutMin) + fTimeoutMin) * time.Millisecond
}

func (n *Node) sendVote(dest int) {
	var m Message
	m.mType = Vote
	m.src = n.nodeHBState.id
	m.term = n.electionState.term
	n.nodeChannels[dest] <- m
	n.electionState.votedThisTerm = true
	//fmt.Println(n.nodeHBState.id, "voted for", dest, "term", n.electionState.term, time.Now())
}

func (n *Node) handleElection() {
	if n.electionState.nodeState == follower {
		if time.Since(n.electionState.timeoutStart) > n.electionState.timeoutDuration {
			n.electionState.nodeState = candidate
			n.electionState.currentLeader = -1
			n.startElection()
		}
	} else if n.electionState.nodeState == candidate {
		if time.Since(n.electionState.timeoutStart) > n.electionState.timeoutDuration {

			if n.electionState.voteCount > NumNodes/2 {
				n.becomeLeader()
			} else {
				n.becomeFollower(n.electionState.term)
			}
		}
	} else if n.electionState.nodeState == leader {
		if time.Since(n.electionState.timeoutStart) > n.electionState.timeoutDuration {
			n.sendLeaderHB()
			n.electionState.timeoutStart = time.Now()
			n.electionState.timeoutDuration = time.Duration(leaderHBTimeout) * time.Millisecond
			/*
			fmt.Println("simulating leader", n.id, "going down for", fTimeoutMax, "ms at",
				time.Now())
			time.Sleep(time.Duration(fTimeoutMax) * time.Millisecond)
			 */
		}
		n.clusterHBState.handleMembership(n.clusterChannels, NumClusters)
	}
}

func (n *Node) startElection() {
	n.electionState.currentLeader = -1
	n.electionState.term++
	n.electionState.timeoutStart = time.Now()
	n.electionState.timeoutDuration = time.Duration(electionDuration) * time.Millisecond
	// vote for self
	n.electionState.voteCount = 1
	n.electionState.votedThisTerm = true
	//fmt.Println(n.nodeHBState.id, "became candidate at", n.electionState.term, time.Now())
	n.requestVotes()
}

func (n *Node) requestVotes() {
    var m Message
    m.mType = VoteRequest
    if len(n.logTable) != 0 {
        m.logIndex = len(n.logTable)-1
        m.logTerm = n.logTable[len(n.logTable)-1].term
    } else {
        m.logIndex = 0
        m.logTerm = 0
    }
	n.broadcastMessage(m)
}

func (n *Node) broadcastMessage(m Message) {
	m.src = n.nodeHBState.id
	m.term = n.electionState.term

	for i := 0; i < len(n.nodeChannels); i++ {
		if i != n.nodeHBState.id && n.nodeHBState.hbTable[i].alive {
			n.nodeChannels[i] <- m
		}
	}
}

func (n *Node) becomeLeader() {
	n.electionState.nodeState = leader
	n.electionState.currentLeader = n.nodeHBState.id
	n.sendLeaderHB()
	n.electionState.timeoutStart = time.Now()
	n.electionState.timeoutDuration = time.Duration(leaderHBTimeout) * time.Millisecond
	fmt.Println("cluster", n.clusterHBState.id, n.nodeHBState.id, "became leader at", n.electionState.term,
		time.Now())
}

func (n *Node) sendLeaderHB() {
	n.broadcastMessage(Message{mType : LeaderHB, hbTable: n.clusterHBState.hbTable})
}