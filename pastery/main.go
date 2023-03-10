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


			// TODO: Before Inserting check is slot is present
			// it is used to store resource as the connected sockets are
			// never disconnected
			for k, _ := range rts {
				conn, err := net.Dial("tcp", k)
				if err != nil {
					log.Println(err)
					continue
				}

				fmt.Fprintf(conn, "notify\n")
				fmt.Fprintf(conn, "%s %s\n", selfAddr, selfNodeId.hash)

				scanner := bufio.NewScanner(conn)
				addr, id, err := parseAddress(scanner)
				if err != nil {
					log.Println(err)
					continue
				}

				t := ConnectedNodeInfo {
					Conn: conn,
					Addr: addr,
					NodeId: id,
				}

				p := idPrefixCount(id)
				d, _ := hexIdx(id.hash[p])

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
				addr, id, err := parseAddress(scanner)
				if err != nil {
					log.Println(err)
					continue
				}

				t := ConnectedNodeInfo {
					Conn: conn,
					Addr: addr,
					NodeId: id,
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
	var nodeInfo *ConnectedNodeInfo
	defer cleanup(nodeInfo)
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

func cleanup(info *ConnectedNodeInfo) {
	// TODO:
	// Remove from routing table
	// remove from leaf node
}

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
