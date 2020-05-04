//CSC 469 Week 3 Assignment 
//Ayan Patel

package main

import (
	"fmt"
	"math/rand"
	"time"
	"sync"
	"strconv"
	"bytes"
)

const num_nodes = 8
const num_neighbors = 2

type hb struct {
	hb int
	t time.Time
	failed bool
}

type message struct {
	hbt map[int]*hb
	dest int
}

func node(id int, message_ch chan message, wg *sync.WaitGroup) {
	defer wg.Done()

	hbt := make(map[int]*hb)

	var neighbors [num_neighbors]int

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < num_neighbors; i++ {
		neighbors[i] = (id + i + 1) % num_nodes
	}

	hbt[id] = &hb{0, time.Now(), false}

	Xstart := time.Now()
	Ystart := time.Now()
	Zstart := time.Now()
	X := float64(1)
	Y := float64(2)
	Z := float64(15 * (id+1))
	Tf := float64(10)
	Tc := float64(20)

	for {
		//Increment heartbeat counter and print table
		if time.Since(Xstart).Seconds() >= X {
			hbt[id].hb++
			hbt[id].t = time.Now()
			Xstart = time.Now()
			//fmt.Println("Node:", id, "hbt:", hbt)
			var buffer bytes.Buffer
			buffer.WriteString("Node: " + strconv.Itoa(id) + "\n")
			for hbt_id, hbt_hb := range hbt {
				buffer.WriteString("  Node:" + strconv.Itoa(hbt_id) + " hb:" + strconv.Itoa(hbt_hb.hb) + " time:" + hbt_hb.t.Format("15:04:05") + " failed:" + strconv.FormatBool(hbt_hb.failed) + "\n")
			}
			fmt.Println(buffer.String())
		}
		
		//Send heartbeat table to a neighboring node
		if time.Since(Ystart).Seconds() >= Y {
			rand_idx := rand.Intn(num_neighbors)
			neighbor := neighbors[rand_idx]
			if neighbor < 0 {
				rand_idx += 1
				neighbor = neighbors[rand_idx % 2]
			}
			if neighbor >= 0 {
				m := message{hbt: hbt, dest: neighbor}
				message_ch <- m
				fmt.Println("SENT from", id, "to", neighbor)
			}
			Ystart = time.Now()
		}

		//Simulate the node failing 
		if time.Since(Zstart).Seconds() >= Z {
			fmt.Println("NODE", id, "FAILURE")
			break
		}

		active := []int{}
		updateNeighbors := false
		add := true

		//Check for failed nodes and cleanup
		for idx, hbx := range hbt {
			if time.Since(hbx.t).Seconds() >= Tc {
				delete(hbt, idx)
				updateNeighbors = true
			} else if time.Since(hbx.t).Seconds() > Tf {
				hbt[idx].failed = true
				updateNeighbors = true
			} else if idx != id {
				for _, neighbor := range neighbors {
					if neighbor == idx {
						add = false
					}
				}
				if add {
					active = append(active, idx)
				}
				add = true
			}
		}

		//Update neighbors if current neighbor is a failed node
		if updateNeighbors {
			for n_id, neighbor := range neighbors {
				n_hb, found := hbt[neighbor]
				if !found || n_hb.failed {
					if len(active) <= 0 {
						neighbors[n_id] = -1
					} else {
						neighbors[n_id] = active[rand.Intn(len(active))]
					}
				} 
			}
			updateNeighbors = false
		}
		
		//receive a message and update heartbeat table if correct message destination
		select {
		case recv_message := <- message_ch:
			if recv_message.dest == id {
				for r_id,r_hb := range recv_message.hbt {
					if !r_hb.failed {
						if _, found := hbt[r_id]; !found {
							hbt[r_id] = &hb{r_hb.hb, time.Now(), false}
						}
						if r_hb.hb > hbt[r_id].hb {
							hbt[r_id].hb = r_hb.hb
							hbt[r_id].t = time.Now()
						}
					}
				}
			} else {
				message_ch <- recv_message
			}
		default:
		}
	}
}

func main() {
	var wg sync.WaitGroup
	
	message_chan := make(chan message, 32)

	for i := 0; i < num_nodes; i++ {
		wg.Add(1)
		go node(i, message_chan, &wg)
	}

	wg.Wait()
}