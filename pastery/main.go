package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type ConnectedNodeInfo struct {
	net.Conn
	Addr   string
	NodeId string
}

var rwLock sync.RWMutex
var routingTable [32][16]ConnectedNodeInfo
var leafNodesLeft [16]ConnectedNodeInfo
var leafNodesRight [16]ConnectedNodeInfo

// 32 hex digits
var selfAddr = "localhost:8080" // Debug
var selfNodeId string

func main() {
	leafNodesLeft = [16]ConnectedNodeInfo{}
	leafNodesRight = [16]ConnectedNodeInfo{}
	routingTable = [32][16]ConnectedNodeInfo{}

	h := sha256.New()
	h.Write([]byte(selfAddr))
	hash := h.Sum(nil)[:16]
	selfNodeId = fmt.Sprintf("%x", hash)

	log.Println("Self Node Id:", selfNodeId)

	n, err := net.Listen("tcp", selfAddr)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Server Listening At", selfAddr)

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := n.Accept()
		if err != nil {
			log.Println(conn)
			return
		}

		wg.Add(1)
		go func(conn net.Conn) {
			helpNodeToJoin(conn)
			// TODO: Handle get/set requests
			wg.Done()
		}(conn)
	}
}

func helpNodeToJoin(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		log.Println(conn.LocalAddr(), "Join Err")
		return
	}
	/*
	* JOIN struture
	* JOIN addr HashAddr from
	 */
	join := scanner.Text()
	if !strings.HasPrefix(join, "JOIN") {
		log.Println(conn.LocalAddr(), "Expected Join")
		return
	}

	rem := strings.TrimLeft(join, "JOIN")
	// Empty string because of space after join
	addr := strings.Split(rem, " ")[1:]

	if len(addr) != 3 {
		log.Println(conn.LocalAddr(), "Join Structure Error: Not Enough args")
		return
	}

	log.Println("Addr: ", addr[0], "Hash: ", addr[1])

	hash := addr[1]
	prefixCount := idPrefixCount(hash)
	if prefixCount == 32 {
		log.Println(addr[0], "XXX: Hash Collision")
		fmt.Fprintf(conn, "XXX: Hash Collision")
		conn.Close()
		return
	}

	from, err := strconv.Atoi(addr[2])
	if err != nil {
		log.Println(conn.LocalAddr(), "XXX: From Field Err %v", err)
		fmt.Fprintf(conn, "XXX: From Field Err %v", err)
		conn.Close()
		return
	}

	/*
	* RESPONSE:
	* selfAddr, selfNodeId
	* <routing table till matched prefix> ....
	* <Next Closest>
	* if nextClosest == self { <Leaf Noded> }
	 */

	rwLock.Lock()
	defer rwLock.Unlock()

	resp := &strings.Builder{}
	fmt.Fprintf(resp, "%s %s\n", selfAddr, selfNodeId)
	{ // Writing Routing Table Info
		written := 0
		for i := from; i <= prefixCount; i++ {
			for j := 0; j < 16; j++ {
				rtInfo := routingTable[i][j]
				if rtInfo.Conn != nil {
					if written != 0 {
						fmt.Fprintf(resp, ";")
					}
					fmt.Fprintf(resp, "%s %s", rtInfo.Addr, rtInfo.NodeId)
					written += 1
				}
			}
		}
		fmt.Fprintf(resp, "\n")
	}

	// Find the next closest
	bestConn, err := getClosestNode(hash, routingTable[prefixCount], leafNodesLeft, leafNodesRight)
	if err != nil {
		log.Println(addr[0], err)
		fmt.Fprintf(conn, "Internal Server Error: %v", err)
		conn.Close()
		return
	}
	fmt.Fprintf(resp, "%s %s\n", bestConn.Addr, bestConn.NodeId)

	// TODO:
	// Send Leaf Nodes
	strResp := resp.String()

	written := 0
	for written < len(strResp) {
		w, err := conn.Write([]byte(strResp[written:]))
		if err != nil {
			log.Println("Error Writing To Socket", err)
			conn.Close()
			return
		}
		written += w
	}

	d, err := hexIdx(hash[prefixCount])
	if err != nil {
		log.Println(err)
		conn.Close()
		return
	}
	if routingTable[prefixCount][d].Conn == nil {
		routingTable[prefixCount][d] = ConnectedNodeInfo{
			Conn:   conn,
			Addr:   addr[0],
			NodeId: hash,
		}
	} else {
		// TODO:
		// What to do when there already exists a connection in its place??
	}

	r := Compare(hash, selfNodeId)
	if r == 0 {
		log.Println(conn.LocalAddr(), "XXX: Hash Collision")
		fmt.Fprintf(conn, "XXX: Hash Collision")
		conn.Close()
		return
	}

	leafNodes := &leafNodesLeft
	if r > 0 {
		leafNodes = &leafNodesRight
	}

	t := ConnectedNodeInfo{
		Conn:   conn,
		Addr:   addr[0],
		NodeId: hash,
	}

	// It is Lesser than the node
	shiftAtIdx := len(leafNodes)
	for i, n := range leafNodes {
		if n.Conn == nil {
			leafNodes[i] = t
			return
		}
		if t, _ := isComparativelyCloserTo(selfNodeId, hash, n.NodeId, -1); t {
			shiftAtIdx = i
			break
		}
	}

	for i := shiftAtIdx; i < len(leafNodes) && t.Conn != nil; i++ {
		_t := leafNodes[i]
		leafNodes[i] = t
		t = _t
	}
}

func getClosestNode(hash string, tables ...[16]ConnectedNodeInfo) (ConnectedNodeInfo, error) {
	bestConn := ConnectedNodeInfo{
		Conn:   nil,
		Addr:   selfAddr,
		NodeId: selfNodeId,
	}

	prefixCount := idPrefixCount(hash)
	closest := prefixCount

	// Check Leaf Nodes
	for _, table := range tables {
		for i := 0; i < 16; i++ {
			t := table[i]
			if t.Conn == nil {
				continue
			}
			if len(t.NodeId) != 32 {
				runtime.Breakpoint()
			}

			count := idPrefixCountWith(hash, t.NodeId)
			if count == 32 {
				runtime.Breakpoint()
				return ConnectedNodeInfo{}, fmt.Errorf("XXX: Hash Collision")
			}

			if count < closest {
				continue
			}

			isCloser, _ := isComparativelyCloserTo(hash, t.NodeId, bestConn.NodeId, count)
			if isCloser {
				bestConn = t
				closest = count
			}

		}
	}

	return bestConn, nil
}
