package main

import (
	"fmt"
	"math/big"
)

type Hash struct {
	val  *big.Int
	hash string
}

func (h Hash) String() string {
	return h.hash
}

func parseHash(hash string) (*Hash, error) {
	h := &Hash{}
	val, _ := new(big.Int).SetString(hash, 16)
	if val == nil {
		return nil, fmt.Errorf("Parsing Error %v", hash)
	}

	h.val = val
	h.hash = hash
	return h, nil
}

func (h *Hash) isEqual(to *Hash) bool {
	return h.val.Cmp(to.val) == 0
}

var hexNums = []byte("0123456789abcdef")

func hexIdx(c byte) (int, error) {
	for i, h := range hexNums {
		if h == c {
			return i, nil
		}
	}
	return 0, fmt.Errorf("Hex Digit Error %c", c)
}

// Returns true is _hash_ is closer to _to_ compared to _from_ at _at_
func isComparativelyCloserTo(hash, to, from *Hash) bool {
	d1 := new(big.Int)
	d1.Sub(hash.val, to.val)
	d1.Abs(d1)

	d2 := new(big.Int)
	d2.Sub(hash.val, from.val)
	d2.Abs(d2)

	return d1.Cmp(d2) == -1
}

func Compare(hash, with *Hash) int {
	return hash.val.Cmp(with.val)
}

func idPrefixCountWith(h1, h2 *Hash) int {
	hash1 := h1.hash
	hash2 := h2.hash
	for i := 0; i < 32; i++ {
		if hash1[i] != hash2[i] {
			return i
		}
	}
	return 32
}

func idPrefixCount(h *Hash) int {
	selfHash := selfNodeId.hash
	hash := h.hash
	for i := 0; i < 32; i++ {
		if hash[i] != selfHash[i] {
			return i
		}
	}
	return 32
}
