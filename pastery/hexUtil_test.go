package main

import "testing"

func TestHexIdx(t *testing.T) {
	hexNums := "0123456789abcdef"
	for i, j := range hexNums {
		if c, _ := hexIdx(byte(j)); c != i {
			t.Errorf("Hex Idx failed %c: Expected: %d, Got: %d", j, i, c)
		}
	}
}

func TestComparitive(t *testing.T) {
	cases := []struct{
		hash string
		to string
		from string
		res bool
	} {
		{"abcd", "abce", "dddd", true},
		{"abcd", "dddd", "abce", false},
	}
	for _, c := range cases{
		r, _ := isComparativelyCloserTo(c.hash, c.to, c.from, -1)
		if r != c.res {
			t.Errorf("Comparitive Close Function Failed %v", c)
		}
	}

	cases2 := []struct {
		hash string
		to string
		from string
		at int
		res bool 
	} {
		{"abcd", "abce", "abcf", 3, true},
		{"abcd", "abcf", "abce", 3, false},
	}

	for _, c := range cases2{
		r, _ := isComparativelyCloserTo(c.hash, c.to, c.from, c.at)
		if r != c.res {
			t.Errorf("Comparitive Close Function Failed %v", c)
		}
	}
}

/*
func Compare(hash, with string) int {
func idPrefixCountWith(hash1, hash2 string) int {
func idPrefixCount(hash string) int {
*/
