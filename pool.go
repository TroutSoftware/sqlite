package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
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

type savepoint struct {
	name     string
	top      bool
	released bool
	task     *trace.Task
}

var NumThreads = 32

func init() {
	// There is a limit to how many concurrent writes we can issue in SQLite at the same time,
	// even in WAL mode (single writer). Increasing the number too much would still result in busy contention.
	// This takes the same approach as Python's [ThreadPoolExecutor].
	//
	// [ThreadPoolExecutor]: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
	w := runtime.NumCPU() + 4
	if w < 32 {
		NumThreads = w
	}
}

// OpenPool ceates a new connection pool
// TODO(rdo) check if WAL by default is right
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

// Savepoint creates a new [savepoint] in transaction (think about begin).
// If the connection does not exist, it is taken from the pool.
//
// [savepoint]: https://sqlite.org/lang_savepoint.html
func (p *Connections) Savepoint(ctx context.Context) (context.Context, error) {
	ctn, ok := ctx.Value(ckey{}).(*Conn)
	top := false
	if !ok {
		ctn = p.take()
		top = true
		ctn.zombie = time.AfterFunc(30*time.Second, func() {
			slog.Warn("zombie connection detected")
		})
	}

	spn := randname()
	err := ctn.Exec(ctx, "SAVEPOINT "+spn).Err()
	if err != nil {
		return ctx, err
	}
	sp := &savepoint{name: spn, top: top}
	ctx = context.WithValue(ctx, ckey{}, ctn)
	ctx = context.WithValue(ctx, spkey{}, sp)
	ctx, sp.task = trace.NewTask(ctx, "db:sqlite-tx")
	return ctx, nil
}

// Close closes all connections in the pool.
// It can be safely called concurrently [Connections.Savepoint], [Connections.Exec] and [Connections.Release]
// but note that calls to [Connections.Savepoint] or [Connections.Exec] that happen after Close might block forever.
// The mechanism to terminate other connections has to be done out of band.
func (p *Connections) Close() error {
	var err error
	for w := NumThreads; w > 0; w-- {
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

func (p *Connections) Release(ctx context.Context) error {
	if r := recover(); r != nil {
		panic(r)
	}

	ctn := ctx.Value(ckey{}).(*Conn)
	sp := ctx.Value(spkey{}).(*savepoint)
	if sp.released {
		panic("savepoint released twice")
	}

	err := ctn.Exec(ctx, "RELEASE "+sp.name).Err()
	if err == nil {
		if sp.top {
			ctn.zombie.Stop()
			p.put(ctn)
		}
		sp.released = true
		sp.task.End()
	}
	return err
}

func (p *Connections) put(ctn *Conn) {
	connectionsProfiles.Remove(ctn)
	p.mx.Lock()
	ctn.next = p.free
	p.free = ctn

	p.wait.Signal()
	p.mx.Unlock()
}

// RollbackTo rolls back all changes to the current changepoint, but it does
// not release the existing savepoint. This can be useful for retries, as it
// restarts any potential implicit transaction that's related to this
// savepoint.
func (p *Connections) RollbackTo(ctx context.Context) error {
	ctn := ctx.Value(ckey{}).(*Conn)
	sp := ctx.Value(spkey{}).(*savepoint)
	return ctn.Exec(ctx, "ROLLBACK TO "+sp.name).Err()
}

// Rollback is a safety net function that SHOULD be called for EVERY
// savepoint with a defer statement right after the creation of that
// savepoint. If the savepoint has already been released and its changes
// were committed (or merged into the speculative changes of its parent
// savepoint), this function will simply return nil. If this savepoint
// has not yet been released, then this function will first revert any
// changes done within the DB and then release the savepoint. This operation
// may cause the existing transaction to be destroyed, if this savepoint
// was the first savepoint on that connection and there was not other
// explicit transaction that was previously created. If another transaction
// is around (implicit (created by a "BEGIN") or explicit (created by a parent
// savepoint)) that wraps this savepoint, it will still survive.
func (p *Connections) Rollback(ctx context.Context) error {
	ctn := ctx.Value(ckey{}).(*Conn)
	sp := ctx.Value(spkey{}).(*savepoint)
	if sp.released {
		return nil
	}

	err := ctn.Exec(ctx, "ROLLBACK TO "+sp.name).Err()
	if err == nil {
		err = ctn.Exec(ctx, "RELEASE "+sp.name).Err()
	}

	if err == nil {
		sp.released = true
		if sp.top {
			ctn.zombie.Stop()
			p.put(ctn)
		}
		sp.task.End()
	}

	return err
}

func (p *Connections) Exec(ctx context.Context, cmd string, args ...any) *Rows {
	ctn, ok := ctx.Value(ckey{}).(*Conn)
	free := false
	if !ok {
		ctn = p.take()
		free = true
	}
	rows := ctn.Exec(ctx, cmd, args...)
	if free {
		rows.final = func() { p.put(ctn) }
	}

	return rows
}
