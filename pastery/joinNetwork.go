package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
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

	url, id, err := parseAddress(scanner)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Server Info", url, id.hash)

	func() {
		rwLock.Lock()
		defer rwLock.Unlock()
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

	err = parseManyAddress(scanner, rts)
	if err != nil {
		log.Fatal(err)
	}

	closestUrl, closestId, err := parseAddress(scanner)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Next closest Server Info", closestUrl, closestId.hash)

	if closestId.hash == id.hash {
		// Accept leaf nodes
		err := parseManyAddress(scanner, leaf)
		if err != nil {
			log.Fatal(err)
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
