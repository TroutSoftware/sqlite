package sqlite

import (
	"container/ring"
	"sync"
)

var lastQueriesRing = ring.New(30)
var lastQueriesMx sync.Mutex

// RememberQuery can be used to expose any SQL query to the internal `obs_lastqueries` virtual table.
// The last 30 queries will be displayed there.
// This is particularly useful when debugging automatically generated queries.
func RememberQuery(q string) {
	lastQueriesMx.Lock()
	lastQueriesRing.Value = q
	lastQueriesRing = lastQueriesRing.Next()
	lastQueriesMx.Unlock()
}

var ObsLastQueries = RegisterTable("obs_lastqueries", queryRecord{})

type queryRecord struct {
	Query string `vtab:"query"`
}

func (queryRecord) Filter(_ int, _ Constraints) Iter[queryRecord] {
	qs := make([]queryRecord, 0, 30)
	lastQueriesMx.Lock()
	start, p := lastQueriesRing, lastQueriesRing
	for {
		if p.Value != nil {
			qs = append(qs, queryRecord{Query: p.Value.(string)})
		}
		p = p.Next()
		if p == start {
			lastQueriesMx.Unlock()
			return FromArray(qs)
		}
	}
}
