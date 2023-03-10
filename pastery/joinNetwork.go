package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

func joinNetwork(addr string, rts, leaf map[string]struct{}) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	h := sha256.New()
	h.Write([]byte(selfAddr))
	hash := h.Sum(nil)[:16]
	selfNodeId, err = parseHash(fmt.Sprintf("%x", hash))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(conn, "join\n")
	fmt.Fprintf(conn, "%s %x\n", selfAddr, hash[:16])

	/*
	* Response
	* <SERVER URL> <SERVER ID>
	* <ROUTING TABLE> .....
	* <Best SERVER URL known> <SERVER ID>
	* if (best ServerUrl == SERVER URL)
	 */

	// TODO: scanner.Text() may return error
	// check for that
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		log.Fatal("Unexprected Response from server when joining")
	}
	serverInfo := scanner.Text()
	url, id, found := strings.Cut(serverInfo, " ")
	if !found {
		log.Fatalf("Unexprected Response from server when joining : Server address %s", serverInfo)
	}
	log.Println("Server Info", url, id)

	func() {
		rwLock.Lock()
		defer rwLock.Unlock()
		id, err := parseHash(id)
		if err != nil {
			log.Println(err)
			return
		}
		t := ConnectedNodeInfo{
			Conn:   conn,
			Addr:   url,
			NodeId: id,
		}

		prefixCount := idPrefixCount(id)
		if prefixCount == 32 {
			log.Fatal("XXX: Hash Collision") // EH???
		}

		d, err := hexIdx(id.hash[prefixCount])
		if err != nil {
			log.Println(err)
			return
		}
		insertInRoutingTable(prefixCount, d, t)
		insertInLeafNode(t)
	}()

	if !scanner.Scan() {
		log.Fatal("Expected from server: Routing Table : Scanner Failed")
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	serverInfo = scanner.Text()
	routingTableInfo := strings.Split(serverInfo, ";")
	for _, rtInfo := range routingTableInfo {
		addr, _, found := strings.Cut(rtInfo, " ")
		if !found {
			log.Println(rtInfo)
			return
		}

		if addr == selfAddr {
			continue
		}
		log.Println("Got: ", addr, id)
		rts[addr] = struct{}{}
	}

	if !scanner.Scan() {
		log.Fatal("Unexprected Response from server when joining")
	}
	serverInfo = scanner.Text()
	closestUrl, closestId, found := strings.Cut(serverInfo, " ")
	if !found {
		log.Fatalf("Unexprected Response from server when joining : Server address %s", serverInfo)
	}
	log.Println("Server Info", closestUrl, closestId)

	if closestId == id {
		// Accept leaf nodes
		if !scanner.Scan() {
			log.Fatal("Expected from server: Routing Table : Scanner Failed")
		}

		serverInfo = scanner.Text()
		routingTableInfo := strings.Split(serverInfo, ";")
		for _, rtInfo := range routingTableInfo {
			addr, _, found := strings.Cut(rtInfo, " ")
			if !found {
				log.Println(rtInfo)
				break
			}
			if addr == selfAddr {
				continue
			}
			leaf[addr] = struct{}{}
		}
	} else {
		joinNetwork(closestUrl, rts, leaf)
		delete(rts, closestUrl)
		delete(leaf, closestUrl)
	}

	GlobalWaitGroup.Add(1)
	go func() {
		defer GlobalWaitGroup.Done()
		handleConnection(conn)
	}()
}
