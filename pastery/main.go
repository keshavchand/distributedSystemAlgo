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

var keyToConnLock sync.RWMutex
var keyToConn map[string]net.Conn

// 32 hex digits
var selfAddr string
var selfNodeId *Hash

var GlobalWaitGroup sync.WaitGroup

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	keyToConn = make(map[string]net.Conn)

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

				t := ConnectedNodeInfo{
					Conn:   conn,
					Addr:   addr,
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
				}()
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

				t := ConnectedNodeInfo{
					Conn:   conn,
					Addr:   addr,
					NodeId: id,
				}

				insertInLeafNode(t)

				GlobalWaitGroup.Add(1)
				go func() {
					defer GlobalWaitGroup.Done()
					handleConnection(conn)
				}()
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

	//XXX: Error Messages retured to connection are literally useless here
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
				log.Printf("Join Parsing Error: ", err)
				continue
			}
			info.Conn = conn

			nodeInfo = info
			manageInternalJoin(info)
			fmt.Fprintf(conn, resp)

		case "notify":
			err := handleNotify(conn, scanner)
			if err != nil {
				log.Printf("Notify: Parsing Error: %v\n", err)
				continue
			}

		case "put":
			k, v, err := getKV(scanner)
			if err != nil {
				log.Println("GetKV: Paring Error:", err)
				continue
			}

			log.Println("Put Request", k, v)
			cont := func() bool {
				keyToConnLock.RLock()
				defer keyToConnLock.RUnlock()
				_, p := keyToConn[k]
				if p {
					log.Println("Attempt to reinsert value")
					return true
				}
				return false
			}()

			if cont {
				continue
			}

			node, this, err := findClosestNode(k)
			if err != nil {
				log.Println(err)
				continue
			}

			if this {
				Put(k, v)
				fmt.Fprintf(conn, "resp\n%s OK\n", k)
				continue
			}

			go func(node ConnectedNodeInfo, conn net.Conn, key, val string) {
				keyToConnLock.Lock()
				defer keyToConnLock.Unlock()
				keyToConn[key] = conn

				fmt.Fprintf(node.Conn, "put\n%s %s\n", key, val)
			}(node, conn, k, v)

		case "get":
			k, err := getK(scanner)
			if err != nil {
				log.Println("GetK: Paring Error:", err)
				continue
			}

			log.Println("Get Request", k)

			cont := func() bool {
				keyToConnLock.RLock()
				defer keyToConnLock.RUnlock()
				_, p := keyToConn[k]
				if p {
					log.Println("Attempt to reinsert value")
					return true
				}
				return false
			}()

			if cont {
				continue
			}

			node, this, err := findClosestNode(k)
			if err != nil {
				log.Println(err)
				continue
			}

			if this {
				res, err := Get(k)
				if err != nil {
					fmt.Fprintf(conn, "resp\n%s %v\n", k, err)
					continue
				}
				fmt.Fprintf(conn, "resp\n%s %s\n", k, res)
				continue
			}

			go func(node ConnectedNodeInfo, conn net.Conn, key string) {
				keyToConnLock.Lock()
				defer keyToConnLock.Unlock()
				keyToConn[key] = conn

				fmt.Fprintf(node.Conn, "get\n%s\n", key)
			}(node, conn, k)
		case "resp":
			if !scanner.Scan() {
				log.Println("Error nothing in resp")
				continue
			}

			resp := scanner.Text()
			go func(resp string) {
				key, _, found := strings.Cut(resp, " ")
				if !found {
					log.Println("Wrong Response", resp)
					return
				}

				keyToConnLock.Lock()
				defer keyToConnLock.Unlock()
				conn, p := keyToConn[key]
				if !p {
					log.Println("Wrong Response", resp)
					return
				}

				fmt.Fprintf(conn, "resp\n%s\n", resp)
				delete(keyToConn, key)
			}(resp)
		case "exit":
			break
		default:
			fmt.Fprintf(conn, "Unimplemented : %s", command)
		}
	}
}

// NodeId and whether it is self, err
func findClosestNode(key string) (ConnectedNodeInfo, bool, error) {
	hash, err := parseHash(key)
	if err != nil {
		return ConnectedNodeInfo{}, false, err
	}
	count := idPrefixCount(hash)
	if count == 32 {
		return ConnectedNodeInfo{}, true, nil
	}

	rwLock.RLock()
	defer rwLock.RUnlock()
	nodeLeaf, g := searchLeafNodes(hash)
	if g {
		return nodeLeaf, nodeLeaf.Addr == selfAddr, nil
	}

	nodeRt, err := getClosestNode(hash, routingTable[count])
	if isComparativelyCloserTo(hash, nodeLeaf.NodeId, nodeRt.NodeId) {
		return nodeLeaf, nodeLeaf.Addr == selfAddr, nil
	}
	return nodeRt, nodeRt.Addr == selfAddr, nil
}

func getK(scanner *bufio.Scanner) (key string, err error) {
	if !scanner.Scan() {
		err = fmt.Errorf("Wrong Data")
		return
	}
	key = scanner.Text()
	return
}

func getKV(scanner *bufio.Scanner) (key, value string, err error) {
	if !scanner.Scan() {
		err = fmt.Errorf("Wrong Data")
		return
	}

	key, value, found := strings.Cut(scanner.Text(), " ")
	if !found {
		err = fmt.Errorf("Wrong Key Value")
		return
	}

	return
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

// the best leaf Node and the guarantee that this is the best node
func searchLeafNodes(hash *Hash) (ConnectedNodeInfo, bool) {
	res := Compare(selfNodeId, hash)

	bestNode := ConnectedNodeInfo{
		Addr:   selfAddr,
		NodeId: selfNodeId,
	}

	//XXX: This function should be called only
	// after the node is checked for same id
	if res == 0 {
		log.Fatal("Shouldn't be here")
		return bestNode, false
	}

	leafNodes := &leafNodesLeft
	if res > 0 {
		leafNodes = &leafNodesRight
	}

	for _, n := range leafNodes {
		if n.Conn == nil {
			return bestNode, false
		}

		if isComparativelyCloserTo(hash, n.NodeId, bestNode.NodeId) {
			bestNode = n
		} else {
			return n, true
		}
	}
	return bestNode, false
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
