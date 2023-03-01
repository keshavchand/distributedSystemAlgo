package main

import (
	"fmt"
	"math/rand"
	"sync"
)

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
		c1 := make(chan Message, 1)
		c2 := make(chan Message, 1)
		nodes[i].uid = createUid(mapping)


		nodes[i].outBoxRight = c1
		nodes[next].inBoxLeft = c1

		nodes[i].inBoxRight = c2
		nodes[next].outBoxLeft = c2
	}

	return nodes, mapping
}

func reinitNode(n *Node, mapping map[int64]struct{}) {
	delete(mapping, n.uid)
	n.uid = createUid(mapping)
}

func dbgPrintNodes(n []Node) {
	for _, ns := range n {
		fmt.Print(ns.uid, "->")
	}

	fmt.Print(n[0].uid, "\n")
}

const count int64 = 100

func main() {

	nodes, mapping := initNodes(int(count))
	dbgPrintNodes(nodes)

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
