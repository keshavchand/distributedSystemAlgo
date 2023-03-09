package main

import (
	"bufio"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type ConnectedNodeInfo struct {
	net.Conn
	Addr   string
	NodeId *Hash
}

var rwLock sync.RWMutex
var routingTable [32][16]ConnectedNodeInfo
var leafNodesLeft [16]ConnectedNodeInfo
var leafNodesRight [16]ConnectedNodeInfo

// 32 hex digits
var selfAddr string
var selfNodeId *Hash

var GlobalWaitGroup sync.WaitGroup

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	defer GlobalWaitGroup.Wait()
	var addrToConnect string

	// Flag Start
	flag.StringVar(&selfAddr, "addr", "localhost:8081", "Address of this node")
	flag.StringVar(&addrToConnect, "atc", "", "Address to use to join the node")
	flag.Parse()

	// Flag Stop

	leafNodesLeft = [16]ConnectedNodeInfo{}
	leafNodesRight = [16]ConnectedNodeInfo{}
	routingTable = [32][16]ConnectedNodeInfo{}

	h := sha256.New()
	h.Write([]byte(selfAddr))
	hash := h.Sum(nil)[:16]
	var err error
	selfNodeId, err = parseHash(fmt.Sprintf("%x", hash))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Self Node Id:", selfNodeId)

	var wg sync.WaitGroup
	defer wg.Wait()
	if addrToConnect != "" {
		wg.Add(1)
		GlobalWaitGroup.Add(1)
		go func() {
			defer wg.Done()
			defer GlobalWaitGroup.Done()

			rts := make(map[string]struct{})
			leaf := make(map[string]struct{})
			joinNetwork(addrToConnect, rts, leaf)

			delete(rts, addrToConnect)
			delete(leaf, addrToConnect)

			for k, _ := range rts {
				conn, err := net.Dial("tcp", k)
				if err != nil {
					log.Println(err)
					continue
				}

				fmt.Fprintf(conn, "notify\n")
				fmt.Fprintf(conn, "%s %s\n", selfAddr, selfNodeId.hash)

				scanner := bufio.NewScanner(conn)
				if !scanner.Scan() { continue /* TODO: Error Checking */ }
				data := strings.Split(scanner.Text(), " ")

				hash, _ := parseHash(data[1])

				t := ConnectedNodeInfo {
					Conn: conn,
					Addr: data[0],
					NodeId: hash,
				}

				p := idPrefixCount(hash)
				d, _ := hexIdx(hash.hash[p])

				insertInRoutingTable(p, d, t)
				insertInLeafNode(t)

				GlobalWaitGroup.Add(1)
				go func() {
					defer GlobalWaitGroup.Done()
					handleConnection(conn)
				} ()
			}

			for k, _ := range leaf {
				if _, ok := rts[k]; ok {
					continue
				}
				conn, err := net.Dial("tcp", k)
				if err != nil {
					log.Println(err)
					continue
				}

				fmt.Fprintf(conn, "notify\n")
				fmt.Fprintf(conn, "%s %s\n", selfAddr, selfNodeId.hash)

				scanner := bufio.NewScanner(conn)
				if !scanner.Scan() { continue /* TODO: Error Checking */ }
				data := strings.Split(scanner.Text(), " ")
				hash, _ := parseHash(data[1])

				t := ConnectedNodeInfo {
					Conn: conn,
					Addr: data[0],
					NodeId: hash,
				}

				insertInLeafNode(t)

				GlobalWaitGroup.Add(1)
				go func() {
					defer GlobalWaitGroup.Done()
					handleConnection(conn)
				} ()
			}
		}()
	}

	n, err := net.Listen("tcp", selfAddr)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Server Listening At", selfAddr)

	for {
		conn, err := n.Accept()
		if err != nil {
			log.Println(conn)
			return
		}

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer func() {
				log.Println("Closing Connection")
				err := conn.Close()
				if err != nil {
					log.Println("Closing Connection", err)
				}
			}()
			handleConnection(conn)
		}(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer cleanup(conn)
	var nodeInfo *ConnectedNodeInfo
	// cleanup routing table and leaf node
	// after removal
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.ToLower(scanner.Text())
		switch command {
		case "join":
			if nodeInfo != nil {
				fmt.Fprintf(conn, "Already Joined\n")
				continue
			}

			info, resp, err := helpNodeToJoin(scanner)
			if err != nil {
				fmt.Fprintf(conn, "Parsing Error %v", err)
				continue
			}
			info.Conn = conn

			nodeInfo = info
			manageInternalJoin(info)
			fmt.Fprintf(conn, resp)
		case "notify":
			err := handleNotify(conn, scanner)
			if err != nil {
				fmt.Fprintf(conn, "Parsing Error %v", err)
				log.Printf("Parsing Error %v", err)
				continue
			}
		case "exit":
			break
		default:
			fmt.Fprintf(conn, "Unimplemented : %s", command)
		}
	}
}

func cleanup(conn net.Conn) {}

func handleNotify(conn net.Conn, scanner *bufio.Scanner) error {
	/*
	* Notify Structure
	* <Addr> <Info>
	*
	* reply:
	* <Self Addr> <Info>
	*/
	if !scanner.Scan() {
		log.Println("Can't read from scanner")
		return fmt.Errorf("Can't read from scanner")
	}

	joinAddr := scanner.Text()
	addr := strings.Split(joinAddr, " ")

	if len(addr) < 2 {
		return fmt.Errorf("Wrong Structure")
	}

	log.Println("Addr: ", addr[0], "Hash: ", addr[1])
	hash, err := parseHash(addr[1])
	if err != nil {
		return err
	}
	prefixCount := idPrefixCount(hash)
	if prefixCount == 32 {
		return fmt.Errorf("Hash Collision")
	}

	d, err := hexIdx(hash.hash[prefixCount])
	if err != nil {
		return err
	}

	info := ConnectedNodeInfo{
		Conn:   conn,
		Addr:   addr[0],
		NodeId: hash,
	}

	rwLock.Lock()
	defer rwLock.Unlock()
	if !insertInRoutingTable(prefixCount, d, info) {
		log.Println("Can't Insert Collision with ", info.NodeId.hash,
			routingTable[prefixCount][d].NodeId.hash)
	}
	insertInLeafNode(info)

	fmt.Fprintf(conn, "%s %s\n", selfAddr, selfNodeId.hash)
	log.Println("Connected ", addr, hash)
	return nil
}

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

	func () {
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
	} ()

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
		rts[addr] = struct{} {}
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
			leaf[addr] = struct{} {}
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

func getClosestNode(hash *Hash, tables ...[16]ConnectedNodeInfo) (ConnectedNodeInfo, error) {
	bestConn := ConnectedNodeInfo{
		Conn:   nil,
		Addr:   selfAddr,
		NodeId: selfNodeId,
	}

	// Check Leaf Nodes
	for _, table := range tables {
		for i := 0; i < 16; i++ {
			t := table[i]
			if t.Conn == nil {
				continue
			}

			isCloser := isComparativelyCloserTo(hash, t.NodeId, bestConn.NodeId)
			if isCloser {
				bestConn = t
			}

		}
	}

	return bestConn, nil
}
