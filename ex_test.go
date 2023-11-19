package sqlite_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TroutSoftware/sqlite"
)

// whatTimeIsIt defines the structure of the virtual table.
// Location is set as:
//   - filtered, so it will be made available to the Filter method
//   - hidden, so it can be called as a function in SQL
//
// Typical code will want to annotate those structures with a call to generate accessors to all interesting fields.
//
//	//go:generate constrainer whatTimeIsIt
type whatTimeIsIt struct {
	Location string `vtab:"location,filtered,hidden"`
	Time     string `vtab:"time"`
}

var epoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// WhatTimeIsItAPI provides the (very important) service to know a point in time at a given location.
func WhatTimeIsItAPI(where *time.Location) string {
	return epoch.In(where).Format(time.Kitchen)
}

// This function is typically generated in its own file whatTimeIsIt_access.go.
func (r whatTimeIsIt) GetLocation(cs sqlite.Constraints) (v string, ok bool) {
	match := 0
	for _, c := range cs {
		if c.Column == "location" {
			if c.Operation == sqlite.ConstraintEQ {
				v = c.ValueString()
				match++
			} else {
				panic("Value getter with non-constrained values")
			}
		}
	}
	if match == 0 {
		return v, false
	}
	if match == 1 {
		return v, true
	}

	panic("more than one match")
}

func (wtc whatTimeIsIt) Filter(_ int, cs sqlite.Constraints) sqlite.Iter[whatTimeIsIt] {
	loc := time.UTC
	if lspec, ok := wtc.GetLocation(cs); ok {
		var err error
		if loc, err = time.LoadLocation(lspec); err != nil {
			return sqlite.FromError[whatTimeIsIt](err)
		}
	}

	now := WhatTimeIsItAPI(loc)
	return sqlite.FromOne(whatTimeIsIt{
		Location: loc.String(),
		Time:     now,
	})
}

/*
Virtual tables can represent much more than data on disk

In this example a virtual table is used to get the time in a local-dependent fashion.
*/
func ExampleRegisterTable() {
	db, err := sqlite.Open(sqlite.MemoryPath, sqlite.RegisterTable("whattimeisit", whatTimeIsIt{}))
	if err != nil {
		log.Fatal(err)
	}

	var kitchenclock string
	err = db.Exec(context.Background(), "select time from whattimeisit('Europe/Dublin')").ScanOne(&kitchenclock)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("epoch time", kitchenclock)
	// Output: epoch time 12:00AM
}
