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
	parse := func (s string) *Hash {
		r, e := parseHash(s)
		if e != nil{
			panic(e)
		}
		return r
	}

	cases := []struct{
		hash *Hash
		to *Hash
		from *Hash
		res bool
	} {
		{parse("abcd"), parse("abce"), parse("dddd"), true},
		{parse("abcd"), parse("dddd"), parse("abce"), false},
		{parse("c8"), parse("d1"), parse("c7"), false},
	}
	for i, c := range cases{
		r := isComparativelyCloserTo(c.hash, c.to, c.from)
		if r != c.res {
			t.Errorf("%d: Comparitive Close Function Failed %s %s %s",
				i,
				c.hash.val.String(),
				c.to.val.String(),
				c.from.val.String(),
			)
		}
	}
}

/*
func Compare(hash, with string) int {
func idPrefixCountWith(hash1, hash2 string) int {
func idPrefixCount(hash string) int {
*/
