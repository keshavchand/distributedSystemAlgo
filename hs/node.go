package main

import (
	"context"
	"log"
)

type Direction int

const (
	LEFT Direction = iota
	RIGHT
)

var Iter int64

type Message struct {
	uid       int64
	origHops  int64
	hops      int64
	direction Direction

	suggest   bool // Suggest / (Selected)'
	leaderCtx context.Context

	Iter int64
}

type Node struct {
	isLeader bool
	uid      int64

	ctx        context.Context
	outBoxLeft chan<- Message
	inBoxLeft  <-chan Message

	outBoxRight chan<- Message
	inBoxRight  <-chan Message
}

func (n *Node) broadcast(hops int64) {
	msg := Message{
		uid:       n.uid,
		origHops:  hops,
		hops:      hops,
		direction: LEFT,

		Iter: Iter,

		suggest: true,
	}
	n.outBoxLeft <- msg

	msg.direction = RIGHT
	n.outBoxRight <- msg
}

func (n *Node) isWinner(m Message, d Direction) bool {
	return n.uid == m.uid && m.direction != d
}

func (n *Node) isMaxima(m Message) bool {
	// revert it back
	if m.hops == 1 {
		return m.uid == n.uid
	}
	return false
}

func (n *Node) forward(m Message, d Direction) {
	if m.uid < n.uid {
		return
	}
	if m.hops == 1 {
		m.hops = m.origHops
		if d == RIGHT {
			n.outBoxRight <- m
		} else {
			n.outBoxLeft <- m
		}
		return
	}

	m.hops--
	if d == LEFT {
		n.outBoxRight <- m
	} else {
		n.outBoxLeft <- m
	}
}

func (n *Node) BroadcastWinning() context.CancelFunc {
	leaderCtx, cf := context.WithCancel(context.Background())
	msg := Message{
		uid:       n.uid,
		Iter:      Iter,
		suggest:   false,
		leaderCtx: leaderCtx,
	}
	n.outBoxRight <- msg
	return cf
}

func (n *Node) runElection() context.CancelFunc {
	hops := 1

	cancel := func() {}

	n.isLeader = false
	for !n.isLeader {
		// log.Println("Running For Hops", hops)
		n.broadcast(int64(hops))

		isLargestInLeft := false
		isLargestInRight := false

		for !isLargestInLeft || !isLargestInRight {
			select {
			case m := <-n.inBoxLeft:
				if m.Iter < Iter {
					continue
				}

				if !m.suggest {
					if m.uid != n.uid {
						n.ctx = m.leaderCtx
						n.outBoxRight <- m
					}

					return cancel
				}


				// Accoring to hs if the message came from other side
				// we won the algorithm
				if n.isWinner(m, LEFT) {
					isLargestInRight = true
					if !n.isLeader {
						n.isLeader = true
						log.Println("Brodcasting", n.uid)
						cancel = n.BroadcastWinning()
					}
				} else if n.isMaxima(m) {
					isLargestInLeft = true
				} else {
					n.forward(m, LEFT)
				}

			case m := <-n.inBoxRight:
				if m.Iter < Iter {
					continue
				}
				if n.isWinner(m, RIGHT) {
					isLargestInLeft = true
					if !n.isLeader {
						n.isLeader = true
						log.Println("Brodcasting", n.uid)
						cancel = n.BroadcastWinning()
					}
				} else if n.isMaxima(m) {
					isLargestInRight = true
				} else {
					n.forward(m, RIGHT)
				}
			}
		}
		hops <<= 1
	}

	return cancel
}

var sg1 SyncBarrier = newSyncBarrier(count)
var sg2 SyncBarrier = newSyncBarrier(count)

func (n *Node) start() {
	for {
		//XXX: This is a hack to prevent goroutines from deadlocking
		// Apparently there is some zombie messages in the queue 
		// that causes the program to deadlock
		drainChannel:
		for {
			select {
			case m := <-n.inBoxLeft:
				log.Println("LEFT", m)
			case m := <-n.inBoxRight:
				log.Println("RIGHT", m)
			default:
				break drainChannel
			}
		}

		sg1.Wait()
		cancel := n.runElection()

		if n.isLeader {
			log.Println("------------------Leader Selected: ", n.uid)

			sg2.Wait()
			cancel()
			Iter++
			log.Println("Leader Died: ", n.uid)
			return
		}

		sg2.Wait()
		<-n.ctx.Done() // Leader died
	}
}
