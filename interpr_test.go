package sqlite

import (
	"testing"
)

func TestIsComplete(t *testing.T) {
	cases := []struct {
		st       string
		complete bool
	}{
		{"", false},
		{"select *", false},
		{"select * from table", false},
		{"select * from table;", true},
	}

	for _, c := range cases {
		if got := IsComplete(c.st); got != c.complete {
			t.Errorf("IsComplete(%s): want %t, got %t", c.st, c.complete, got)
		}
	}
}
