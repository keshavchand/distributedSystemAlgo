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
