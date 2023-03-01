package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
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

	ctx context.Context

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

	iter := Iter
	cancel := func() {}
	n.isLeader = false

	seenOneWinnerMessageBefore := false

	var wg sync.WaitGroup
	defer wg.Wait()
	for !n.isLeader {
		n.broadcast(int64(hops))

		isLargestInLeft := false
		isLargestInRight := false

		for !isLargestInLeft || !isLargestInRight {
			select {
			case m := <-n.inBoxLeft:
				if m.Iter < iter {
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
					if !seenOneWinnerMessageBefore {
						seenOneWinnerMessageBefore = true
						continue
					}

					isLargestInRight = true
					if !n.isLeader {
						n.isLeader = true
						cancel = n.BroadcastWinning()
					}
				} else if n.isMaxima(m) {
					isLargestInLeft = true
				} else {
					wg.Add(1)
					// XXX: Sometime writing to channel of opposite
					// goroutine makes both of them deadlock
					go func(m Message) {
						n.forward(m, LEFT)
						wg.Done()
					}(m)
				}

			case m := <-n.inBoxRight:
				if m.Iter < iter {
					continue
				}

				if n.isWinner(m, RIGHT) {
					if !seenOneWinnerMessageBefore {
						seenOneWinnerMessageBefore = true
						continue
					}

					isLargestInLeft = true
					if !n.isLeader {
						n.isLeader = true
						cancel = n.BroadcastWinning()
					}
				} else if n.isMaxima(m) {
					isLargestInRight = true
				} else {
					wg.Add(1)
					go func(m Message) {
						n.forward(m, RIGHT)
						wg.Done()
					}(m)
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
		sg1.Wait()
		cancel := n.runElection()

		if n.isLeader {
			log.Println("Leader Selected: ", n.uid)
			log.Println("ITER: -----------------------------------", Iter)

			sg2.Wait()
			atomic.AddInt64(&Iter, 1)
			cancel()
			return
		}

		sg2.Wait()
		<-n.ctx.Done() // Leader died
	}
}
