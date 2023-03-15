package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"
)

func parseAddress(scanner *bufio.Scanner) (add string, id *Hash, err error) {
	if !scanner.Scan() {
		log.Println("Can't read from scanner")
		err = fmt.Errorf("Can't read from scanner")
		return
	}

	joinAddr := scanner.Text()
	// Empty string because of space after join
	addr := strings.Split(joinAddr, " ")

	if len(addr) < 2 {
		err = fmt.Errorf("Wrong Structure")
		return
	}

	hash, parseErr := parseHash(addr[1])
	if parseErr != nil {
		return
	}
	prefixCount := idPrefixCount(hash)
	if prefixCount == 32 {
		err = fmt.Errorf("Hash Collision")
		return
	}

	return addr[0], hash, nil
}

func parseManyAddress(scanner *bufio.Scanner, addr map[string]struct{}) error{
	if !scanner.Scan() {
		return fmt.Errorf("Empty Data Stream")
	}

	serverInfo := scanner.Text()
	routingTableInfo := strings.Split(serverInfo, ";")

	for _, rtInfo := range routingTableInfo {
		a, _, found := strings.Cut(rtInfo, " ")
		if !found {
			log.Println(rtInfo)
			continue
		}

		if a == selfAddr {
			continue
		}

		addr[a] = struct{} {}
	}

	return nil
}
