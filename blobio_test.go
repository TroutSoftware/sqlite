package sqlite

import (
	"context"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBlobIO(t *testing.T) {
	db, err := Open(t.TempDir() + "/linkedlist")
	if err != nil {
		t.Fatal(err)
	}

	// linked list binary format (len & next fixed size big endian u32)
	// +---+----+------------------+
	// | len | prev | utf8 value                 |
	// +---+----+------------------+

	if err := db.Exec(context.Background(), "create table linked_list(id integer primary key, value text)").Err(); err != nil {
		t.Fatal(err)
	}

	values := []string{
		"computable numbers", "Entscheidungsproblem",
		"axiomatic system", "Vollständigkeitssatz",
	}

	rnum := 0
	for _, v := range values {
		blob, err := db.CreateBLOB("main", "linked_list", "value", len(v)+8)
		if err != nil {
			t.Fatal(err)
		}

		hsz := make([]byte, 4)
		binary.BigEndian.PutUint32(hsz, uint32(len(v)))
		blob.WriteAt(hsz, 0)

		binary.BigEndian.PutUint32(hsz, uint32(rnum))
		blob.WriteAt(hsz, 4)

		blob.WriteAt([]byte(v), 8)

		rnum = blob.RowNumber
		if err := blob.Close(); err != nil {
			t.Fatal(err)
		}
	}

	var got []string

	for rnum != 0 {
		blob, err := db.OpenBLOB("main", "linked_list", "value", rnum)
		if err != nil {
			t.Fatal(err)
		}

		hsz := make([]byte, 4)
		blob.ReadAt(hsz, 0)
		sz := binary.BigEndian.Uint32(hsz)

		buf := make([]byte, sz)
		blob.ReadAt(buf, 8)
		got = append(got, string(buf))

		blob.ReadAt(hsz, 4)
		rnum = int(binary.BigEndian.Uint32(hsz))

		if err := blob.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// technically a bit weaker (walking the linked list is reversed), but enough for the test
	sort.Strings(values)
	sort.Strings(got)

	if !cmp.Equal(got, values) {
		t.Error(cmp.Diff(got, values))
	}
}
