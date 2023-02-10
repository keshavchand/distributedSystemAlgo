package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
)

type Message struct {
	uid       int64
	suggest   bool // Suggest / (Selected)'
	leaderCtx context.Context
}

type Node struct {
	isLeader bool
	uid      int64

	ctx    context.Context
	outBox chan<- Message
	inBox  <-chan Message
}

func (n *Node) ProposeSelfLeader() {
	var m = Message{
		uid:     n.uid,
		suggest: true,
	}

	n.outBox <- m
}

func (n *Node) DeclareSelfLeader() context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	var m = Message{
		uid:       n.uid,
		suggest:   false,
		leaderCtx: ctx,
	}

	n.outBox <- m
	return cancel
}

func (n *Node) runElection() context.CancelFunc {
	n.isLeader = false
	n.ProposeSelfLeader()

	cancel := func() {}
	for val := range n.inBox {

		if !val.suggest {
			if val.uid != n.uid {
				// Leader has been selected
				n.isLeader = false
				n.ctx = val.leaderCtx
				n.outBox <- val
			}
			// we are the leader
			// just return the cancel function
			return cancel
		}

		if val.uid == n.uid {
			// We won the election
			// brodcast it to everyone
			n.isLeader = true
			cancel = n.DeclareSelfLeader()
		}

		if val.uid > n.uid {
			// We lost the election
			// just pass the leader's uid
			n.outBox <- val
		}

		// Someone worse than us appeared
		// ignored
	}

	// Why would we close the inBox ???
	return cancel
}

func (n *Node) start() {
	for {
		cancel := n.runElection()

		if n.isLeader {
			log.Println("Leader Selected: ", n.uid)
			cancel()
			log.Println("Leader Died: ", n.uid)
			return
		}

		<-n.ctx.Done() // Leader died
	}
}

func createUid(m map[int64]struct{}) int64 {
	var uid int64
	for {
		uid = rand.Int63()
		_, ok := m[uid]
		if !ok {
			m[uid] = struct{}{}
			break
		}
	}

	return uid
}

func initNodes(c int) ([]Node, map[int64]struct{}) {
	mapping := make(map[int64]struct{})

	nodes := make([]Node, c)
	for i, _ := range nodes {
		next := (i + 1) % len(nodes)
		c := make(chan Message, 1)

		uid := createUid(mapping)
		nodes[i].uid = uid
		nodes[i].outBox = c
		nodes[next].inBox = c
	}

	return nodes, mapping
}

func reinitNode(n *Node, mapping map[int64]struct{}) {
	delete(mapping, n.uid)
	n.uid = createUid(mapping)
}

func main() {
	nodes, mapping := initNodes(2000)

	var m sync.Mutex
	for _, n := range nodes {
		go func(n Node) {
			for {
				n.start()

				m.Lock()
				reinitNode(&n, mapping)
				m.Unlock()
			}
		}(n)
	}

	select {}
}
