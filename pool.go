package sqlite

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

// #include <amalgamation/sqlite3.h>
// #include <stdint.h>
//
// extern char * go_strcpy(_GoString_ st);
// extern void go_free(void*);
// extern int sqlite_BindGoPointer(sqlite3_stmt* stmt, int pos, uintptr_t ptr, const char* name);
import "C"

const MemoryPath = "file::memory:?mode=memory"

var connectionsProfiles = pprof.NewProfile("t.sftw/sqlite/connections")

// ZombieTimeout is the time after which a transaction is considered leaked
var ZombieTimeout = 30 * time.Second

// Connections is a pool of connections to a single SQLite database.
type Connections struct {
	free *Conn      // free list
	mx   sync.Mutex // protects all above

	wait sync.Cond
}

// FreeCount returns the number of free connections in the pool
func (c *Connections) FreeCount() int {
	c.mx.Lock()
	defer c.mx.Unlock()
	i := 0
	for p := c.free; p != nil; p = p.next {
		var st *C.sqlite3_stmt
		st = C.sqlite3_next_stmt(p.db, st)
		if st != nil {
			q := C.sqlite3_sql(st)
			fmt.Println("dangling statements! ", C.GoString(q))
		}
		i++
	}
	return i
}

type ckey struct{}
type spkey struct{}

var NumThreads int

func init() {
	// There is a limit to how many concurrent writes we can issue in SQLite at the same time,
	// even in WAL mode (single writer). Increasing the number too much would still result in busy contention.
	// This takes the same approach as Python's [ThreadPoolExecutor].
	//
	// [ThreadPoolExecutor]: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
	NumThreads = min(runtime.NumCPU()+4, 32)
}

// OpenPool ceates a new connection pool
func OpenPool(name string, exts ...func(SQLITE3)) (*Connections, error) {
	if name == ":memory:" {
		return nil, errors.New(`":memory:" does not work with pools, use MemoryPath`)
	}

	var pool Connections
	pool.wait = sync.Cond{L: &pool.mx}

	ptr := &pool.free
	for w := NumThreads; w > 0; w-- {
		conn, err := Open(name, exts...)
		if err != nil {
			return nil, err
		}
		if w == 1 {
			var mode string
			err = conn.Exec(context.Background(), "PRAGMA journal_mode=WAL").ScanOne(&mode)
			if err != nil || mode != "wal" {
				return nil, fmt.Errorf("cannot set WAL mode (mode=%s): %w", mode, err)
			}
		}
		*ptr = conn
		ptr = &conn.next
	}

	return &pool, nil
}

func (p *Connections) Exec(ctx context.Context, cmd string, args ...any) *Rows {
	ctn, reused := ctx.Value(ckey{}).(*Conn)
	if !reused {
		ctn = p.take()
	}

	rows := ctn.Exec(ctx, cmd, args...)
	if !reused {
		rows.final = func() { p.put(ctn) }
	}

	return rows
}

// Close closes all connections in the pool.
// It can be safely called concurrently [Connections.Savepoint], [Connections.Exec] and [Connections.Release]
// but note that calls to [Connections.Savepoint] or [Connections.Exec] that happen after Close might block forever.
// The mechanism to terminate other connections has to be done out of band.
func (p *Connections) Close() error {
	var err error
	for range NumThreads {
		ctn := p.take()
		err = errors.Join(err, ctn.Close())
	}

	return err
}

func (p *Connections) take() *Conn {
	p.mx.Lock()
	for p.free == nil {
		p.wait.Wait()
	}

	ctn := p.free
	p.free = ctn.next
	p.mx.Unlock()
	connectionsProfiles.Add(ctn, 2)
	return ctn
}

func (p *Connections) put(ctn *Conn) {
	connectionsProfiles.Remove(ctn)
	p.mx.Lock()
	ctn.next = p.free
	p.free = ctn

	p.wait.Signal()
	p.mx.Unlock()
}
