package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"
)

func helpNodeToJoin(scanner *bufio.Scanner) (*ConnectedNodeInfo, string, error) {
	/*
	* JOIN struture
	* JOIN addr HashAddr
	 */
	if !scanner.Scan() {
		log.Println("Can't read from scanner")
		return nil, "", fmt.Errorf("Can't read from scanner")
	}

	joinAddr := scanner.Text()
	// Empty string because of space after join
	addr := strings.Split(joinAddr, " ")

	if len(addr) < 2 {
		return nil, "", fmt.Errorf("Wrong Structure")
	}

	log.Println("Addr: ", addr[0], "Hash: ", addr[1])
	hash, err := parseHash(addr[1])
	if err != nil {
		return nil, "", err
	}
	prefixCount := idPrefixCount(hash)
	if prefixCount == 32 {
		return nil, "", fmt.Errorf("Hash Collision")
	}

	info := &ConnectedNodeInfo{
		Addr:   addr[0],
		NodeId: hash,
	}

	/*
	* RESPONSE:
	* selfAddr, selfNodeId
	* <routing table till matched prefix> ....
	* <Next Closest>
	* if nextClosest == self { <Leaf Noded> }
	 */

	rwLock.RLock()
	defer rwLock.RUnlock()

	resp := &strings.Builder{}
	fmt.Fprintf(resp, "%s %s\n", selfAddr, selfNodeId.hash)
	{ // Writing Routing Table Info
		written := 0
		for i := 0; i <= prefixCount; i++ {
			for j := 0; j < 16; j++ {
				rtInfo := routingTable[i][j]
				if rtInfo.Conn != nil {
					if written != 0 {
						resp.WriteRune(';')
					}
					fmt.Fprintf(resp, "%s %s", rtInfo.Addr, rtInfo.NodeId)
					written += 1
				}
			}
		}
		resp.WriteRune('\n')
	}

	// Find the next closest
	bestConn, err := getClosestNode(hash, routingTable[prefixCount], leafNodesLeft, leafNodesRight)
	if err != nil {
		return nil, "", fmt.Errorf("ERR: Internal Server Error: %v", err)
	}
	fmt.Fprintf(resp, "%s %s\n", bestConn.Addr, bestConn.NodeId)

	// If this is not the closest node
	// we dont need to send the leaf nodes
	if !bestConn.NodeId.isEqual(selfNodeId) {
		return info, resp.String(), nil
	}

	// writing leaf nodes
	first := true
	for _, n := range leafNodesLeft {
		if n.Conn == nil {
			break
		}
		if !first {
			resp.WriteRune(';')
		}
		fmt.Fprintf(resp, "%s %s", n.Addr, n.NodeId.hash)
		first = false
	}
	for _, n := range leafNodesRight {
		if n.Conn == nil {
			break
		}
		if !first {
			resp.WriteRune(';')
		}
		fmt.Fprintf(resp, "%s %s", n.Addr, n.NodeId.hash)
		first = false
	}

	resp.WriteRune('\n')
	return info, resp.String(), nil
}

func manageInternalJoin(info *ConnectedNodeInfo) error {
	rwLock.Lock()
	defer rwLock.Unlock()

	prefixCount := idPrefixCount(info.NodeId)
	if prefixCount == 32 {
		return fmt.Errorf("Hash Collision")
	}

	d, err := hexIdx(info.NodeId.hash[prefixCount])
	if err != nil {
		return err
	}

	t := ConnectedNodeInfo{
		Conn:   info.Conn,
		Addr:   info.Addr,
		NodeId: info.NodeId,
	}

	success := insertInRoutingTable(prefixCount, d, t)
	if !success {
		log.Println(info.Addr, "Can't insert into table")
		// What to do when there already exists a connection in its place??
	}

	insertInLeafNode(t)
	return nil
}

func insertInLeafNode(info ConnectedNodeInfo) {
	r := Compare(info.NodeId, selfNodeId)
	if r == 0 {
		log.Println("XXX: Hash Collision")
		return
	}

	leafNodes := &leafNodesLeft
	if r > 0 {
		leafNodes = &leafNodesRight
	}

	// It is Lesser than the node
	shiftAtIdx := len(leafNodes)
	for i, n := range leafNodes {
		if n.Conn == nil {
			leafNodes[i] = info
			return
		}
		if n.NodeId.isEqual(info.NodeId) {
			n.Conn.Close()
			leafNodes[i].Conn = info.Conn
			return
		}
		t := isComparativelyCloserTo(selfNodeId, info.NodeId, n.NodeId)
		if t {
			shiftAtIdx = i
			break
		}
	}

	for i := shiftAtIdx; i < len(leafNodes) && info.Conn != nil; i++ {
		_t := leafNodes[i]
		leafNodes[i] = info
		info = _t
	}
}

func insertInRoutingTable(prefixCount, d int, info ConnectedNodeInfo) bool {
	if routingTable[prefixCount][d].Conn == nil {
		routingTable[prefixCount][d] = info

		return true
	} else if routingTable[prefixCount][d].NodeId.isEqual(info.NodeId) {
		log.Println(info.Addr, "Collision")

		routingTable[prefixCount][d].Conn.Close()
		routingTable[prefixCount][d].Conn = info.Conn
		routingTable[prefixCount][d].Addr = info.Addr
		return true
	}

	return false
}
