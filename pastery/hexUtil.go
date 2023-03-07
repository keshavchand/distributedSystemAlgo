package main

import (
	"fmt"
	"math"
)

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
func isComparativelyCloserTo(hash, to, from string, at int) (bool, error) {
	if at == -1 {
		at = 0
		for ; at < len(hash); at++ {
			if hash[at] != to[at] || hash[at] != from[at] {
				break
			}
		}
		if at == len(hash) {
			// all are same
			return false, nil
		}
	}

	if hash[at] == to[at] && hash[at] != from[at] {
		return true, nil
	}
	if hash[at] != to[at] && hash[at] == from[at] {
		return false, nil
	}

	hIdx, fIdx, tIdx := -1, -1, -1
	for i, h := range hexNums {
		if h == hash[at] {
			hIdx = i
		} else if h == from[at] {
			fIdx = i
		} else if h == to[at] {
			tIdx = i
		}
	}

	if hIdx == -1 || fIdx == -1 || tIdx == -1 {
		errHash := hash
		if fIdx == -1 {
			errHash = from
		} else if tIdx == -1 {
			errHash = to
		}
		return false, fmt.Errorf("error in hash values %v", errHash)
	}

	if math.Abs(float64(hIdx-tIdx)) < math.Abs(float64(hIdx-fIdx)) {
		return true, nil
	} else {
		return false, nil
	}
}

func Compare(hash, with string) int {
	i := 0
	for ; i < 32; i++ {
		if hash[i] != with[i] {
			break
		}
	}

	if i == 32 {
		return 0
	}

	hIdx := -1
	wIdx := -1
	for j, h := range hexNums {
		if h == hash[i] {
			hIdx = j
		}
		if h == with[i] {
			wIdx = j
		}
	}

	if hIdx == -1 || wIdx == -1 {
		panic("HANDLE THIS ERROR")
	}

	return hIdx - wIdx
}

func idPrefixCountWith(hash1, hash2 string) int {
	for i := 0; i < 32; i++ {
		if hash1[i] != hash2[i] {
			return i
		}
	}
	return 32
}

func idPrefixCount(hash string) int {
	for i := 0; i < 32; i++ {
		if hash[i] != selfNodeId[i] {
			return i
		}
	}
	return 32
}
