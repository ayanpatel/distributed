package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestHeartbeatAlive(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 1
	TokensPerCluster = 1
	var toks TokenList
	toks = createTokens(toks, 0)
	var clusterChans []chan Message
	clusterChans = make([]chan Message, 1)
	clusterChans[0] = make(chan Message)
	nodes := createNodes(&wg, toks, clusterChans, 0)
	time.Sleep(time.Duration(advertInterval * 4) * time.Millisecond)

	for i := 0; i < len(nodes); i++ {
		for j := 0; j < len(nodes); j++ {
			if !nodes[i].nodeHBState.hbTable[j].alive ||
				nodes[i].nodeHBState.hbTable[j].count == 0 ||
				nodes[i].nodeHBState.hbTable[j].lastUpdated.Equal(time.Time{}) {
				t.Errorf("Heartbeat alive test failed, node %v marked as dead by node %v",
					j, i)
			}
		}
	}
	killAllNodes(nodes)
}

func TestHeartbeatDeath(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 1
	TokensPerCluster = 1
	var toks TokenList
	toks = createTokens(toks, 0)
	var clusterChans []chan Message
	clusterChans = make([]chan Message, 1)
	clusterChans[0] = make(chan Message)
	nodes := createNodes(&wg, toks, clusterChans, 0)
	time.Sleep(time.Duration(advertInterval * 3) * time.Millisecond)
	downNode(0, nodes[0].controlChannel)
	time.Sleep(time.Duration(failInterval * 1.5) * time.Millisecond)

	for i := 1; i < len(nodes); i++ {
		if nodes[i].nodeHBState.hbTable[0].alive {
			t.Errorf("Heartbeat death test failed, node %v did not detect death of node 0",
				i)
		}
	}
	killAllNodes(nodes)
}

func TestHeartbeatCleanup(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 1
	TokensPerCluster = 1
	var toks TokenList
	toks = createTokens(toks, 0)
	var clusterChans []chan Message
	clusterChans = make([]chan Message, 1)
	clusterChans[0] = make(chan Message)
	nodes := createNodes(&wg, toks, clusterChans, 0)
	time.Sleep(time.Duration(advertInterval * 4) * time.Millisecond)
	downNode(0, nodes[0].controlChannel)
	time.Sleep(time.Duration(failInterval * 4) * time.Millisecond)

	for i := 1; i < len(nodes); i++ {
		if  nodes[i].nodeHBState.hbTable[0].alive ||
			nodes[i].nodeHBState.hbTable[0].count != 0 ||
			!nodes[i].nodeHBState.hbTable[0].lastUpdated.Equal(time.Time{}){
			t.Errorf("Heartbeat death test failed, node %v did not cleanup node 0",
				i)
		}
	}
	killAllNodes(nodes)
}

func TestElection(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 1
	TokensPerCluster = 1
	var toks TokenList
	toks = createTokens(toks, 0)
	var clusterChans []chan Message
	clusterChans = make([]chan Message, 1)
	clusterChans[0] = make(chan Message)
	nodes := createNodes(&wg, toks, clusterChans, 0)
	var m Message
	m.mType = Put
	m.data = KeyValue{key:"Jerry Seinfeld", value: "Seinfeld"}
	var m2 Message
	m2.mType = Put
	m2.data = KeyValue{key:"Charlie", value: "Work"}
	nodes[0].clientReqChannel <- m
	time.Sleep(time.Duration(1000) * time.Millisecond)
	nodes[0].clientReqChannel <- m2
	time.Sleep(time.Duration(2000) * time.Millisecond)

	for i := 0; i < len(nodes); i++ {
		fmt.Println(nodes[i].dataStore)
	}

	if nodes[1].dataStore["Jerry Seinfeld"] != "Seinfeld" {
		t.Errorf("Election test failed, key not present in follower")
	}
	killAllNodes(nodes)
}

func TestMultiplePuts(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 1
	TokensPerCluster = 1
	var toks TokenList
	toks = createTokens(toks, 0)
	var clusterChans []chan Message
	clusterChans = make([]chan Message, 1)
	clusterChans[0] = make(chan Message)
	nodes := createNodes(&wg, toks, clusterChans, 0)
	var m1 Message
	m1.mType = Put
	m1.data = KeyValue{key:"Jerry Seinfeld", value: "Seinfeld"}
	var m2 Message
	m2.mType = Put
	m2.data = KeyValue{key:"Charlie Day", value: "IASIP"}
	var m3 Message
	m3.mType = Put
	m3.data = KeyValue{key:"Charlie", value: "Work"}
	nodes[0].clientReqChannel <- m1
	time.Sleep(time.Duration(500) * time.Millisecond)
	nodes[1].clientReqChannel <- m2
	time.Sleep(time.Duration(500) * time.Millisecond)
	nodes[0].clientReqChannel <- m3
	time.Sleep(time.Duration(500) * time.Millisecond)
	fmt.Println(nodes[0].dataStore)
	fmt.Println(nodes[1].dataStore)
	fmt.Println(nodes[2].dataStore)
	fmt.Println(nodes[3].dataStore)
	if nodes[2].dataStore["Jerry Seinfeld"] != "Seinfeld" || nodes[2].dataStore["Charlie Day"] != "IASIP" {
		t.Errorf("Multiple puts test failed, follower does not have both values, reports %v and %v",
			nodes[2].dataStore["Jerry Seinfeld"], nodes[2].dataStore["Charlie Day"])
	}
	if nodes[nodes[0].electionState.currentLeader].dataStore["Charlie"] != "Work" {
		t.Errorf("Multiple puts test failed, leader does not have last value, reports %v",
			nodes[nodes[0].electionState.currentLeader].dataStore["Charlie"])
	}
	killAllNodes(nodes)
}

func TestClusterHeartbeatAlive(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 3
	TokensPerCluster = 4
	var clusterChans []chan Message
	clusterChans = make([]chan Message, NumClusters)
	for i := 0; i < NumClusters; i++ {
		clusterChans[i]	= make(chan Message)
	}

	clusters := createClusters(&wg)
	time.Sleep(time.Duration(2500) * time.Millisecond)

	for i := 0; i < len(clusters); i++ {
		for j := 0; j < len(clusters[i]); j++ {
			//fmt.Println(clusters[i][j].clusterHBState.hbTable)
			for k := 0; k < len(clusters); k++ {
				if  !clusters[i][j].clusterHBState.hbTable[k].alive ||
					clusters[i][j].clusterHBState.hbTable[k].count == 0 ||
					clusters[i][j].clusterHBState.hbTable[k].lastUpdated.Equal(time.Time{}) {
					t.Errorf("Heartbeat alive test failed, cluster %v marked as dead by node %v in cluster %v",
						k, j, i)
				}
			}
		}
	}
	killAllClusters(clusters)
}

func TestClusterPuts(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 3
	TokensPerCluster = 4
	var clusterChans []chan Message
	clusterChans = make([]chan Message, NumClusters)
	for i := 0; i < NumClusters; i++ {
		clusterChans[i]	= make(chan Message)
	}

	clusters := createClusters(&wg)
	for i := 0; i < NumNodes; i++ {
		var m Message
		m.mType = Down
		clusters[1][i].controlChannel <- m
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	var m Message
	m.mType = Put
	m.data = KeyValue{
		key:   "Jerry Seinfeld",
		value: "Seinfeld",
	}
	clusters[0][0].clientReqChannel <- m
	time.Sleep(time.Duration(500) * time.Millisecond)
	m.data = KeyValue{
		key: "Charlie Day",
		value: "IASIP",
	}
	clusters[0][0].clientReqChannel <- m
	time.Sleep(time.Duration(1500) * time.Millisecond)

	/*
	for i := 0; i < NumClusters; i++ {
		for j := 0; j < NumNodes; j++ {
			fmt.Println(clusters[i][j].dataStore)
		}
	}
	 */

	checked := false
	for i := 0; i < NumNodes; i++ {
		if clusters[2][i].electionState.currentLeader == i {
			if clusters[2][i].dataStore["Jerry Seinfeld"] != "Seinfeld" ||
				clusters[2][i].dataStore["Charlie Day"] != "IASIP" {
				t.Errorf("leader node %v in backup cluster %v does not have Jerry and Charlie Data",
					i, 2)
			}
			checked = true
		}
	}

	if !checked {
		t.Errorf("could not find a leader in cluster %v", 2)
	}

	for i := 0; i < NumNodes; i++ {
		var m Message
		m.mType = Up
		clusters[1][i].controlChannel <- m
	}

	time.Sleep(time.Duration(2000) * time.Millisecond)

	/*
	for i := 0; i < NumClusters; i++ {
		for j := 0; j < NumNodes; j++ {
			fmt.Println(clusters[i][j].dataStore)
		}
	}
	 */

	checked = false
	for i := 0; i < NumNodes; i++ {
		if clusters[1][i].electionState.currentLeader == i {
			fmt.Println(i)
			if clusters[1][i].dataStore["Jerry Seinfeld"] != "Seinfeld" ||
				clusters[1][i].dataStore["Charlie Day"] != "IASIP" {
				t.Errorf("leader node %v in owner cluster %v does not have Jerry and Charlie Data",
					i, 1)
			}
			checked = true
		}
	}


	if !checked {
		t.Errorf("could not find a leader in cluster %v", 1)
	}

	killAllClusters(clusters)
}

func TestGet(t *testing.T) {
	var wg sync.WaitGroup
	NumClusters = 1
	TokensPerCluster = 1
	var toks TokenList
	toks = createTokens(toks, 0)
	var clusterChans []chan Message
	clusterChans = make([]chan Message, 1)
	clusterChans[0] = make(chan Message)
	nodes := createNodes(&wg, toks, clusterChans, 0)
	var m1 Message
	m1.mType = Put
	m1.data = KeyValue{key:"Jerry Seinfeld", value: "Seinfeld"}
	var m2 Message
	m2.mType = Put
	m2.data = KeyValue{key:"Charlie", value: "Work"}
	var m3 Message
	m3.mType = Get 
	m3.data = KeyValue{key:"Jerry Seinfeld", value:""}
	nodes[0].clientReqChannel <- m1
	time.Sleep(time.Duration(1000) * time.Millisecond)
	nodes[0].clientReqChannel <- m2
	time.Sleep(time.Duration(1000) * time.Millisecond)
	nodes[0].clientReqChannel <- m3
	time.Sleep(time.Duration(1000) * time.Millisecond)

	for i := 0; i < len(nodes); i++ {
		fmt.Println(nodes[i].dataStore)
	}

	if nodes[nodes[0].electionState.currentLeader].dataStore["Jerry Seinfeld"] != "Seinfeld" {
		t.Errorf("Get test failed, key not present in leader")
	}

	kv := <- nodes[nodes[0].electionState.currentLeader].clientRespChannel
	if kv.value != "Seinfeld" {
		t.Errorf("Get test failed, value not correct in client response")
	}

	killAllNodes(nodes)
}