package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// A quick tour of the most important functions in the library
func Example() {
	ctn, err := Open(MemoryPath)
	if err != nil {
		log.Fatal("cannot open a temporary database", err)
	}

	ctx := context.Background()

	sts := []string{
		"create table tbl1 (a primary key, b)",
		"insert into tbl1 (a, b) values ('hello','world'), ('bonjour','monde'), ('hola','mundo')",
	}
	for _, st := range sts {
		// Exec can be used for direct statements
		if err := ctn.Exec(ctx, st).Err(); err != nil {
			log.Fatal("cannot create table", err)
		}
	}

	// Scan to iterate over all results
	rows := ctn.Exec(ctx, "select a, b from tbl1")
	for rows.Next() {
		var a, b string
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	if rows.Err() != nil {
		log.Fatal("cannot query tbl1", rows.Err())
	}

	// ScanOne is a handy shortcut
	var b string
	if err := ctn.Exec(ctx, "select b from tbl1 where a = ?", "bonjour").ScanOne(&b); err != nil {
		log.Fatal("cannot find value matching \"bonjour\"")
	}
	fmt.Println(b)
	// Output: hello world
	// bonjour monde
	// hola mundo
	// monde
}

func TestShortHands(t *testing.T) {
	ctn, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := ctn.Exec(ctx, "create table tbl1 (a primary key, b)").Err(); err != nil {
		t.Fatal("cannot create table", err)
	}

	if err := ctn.Exec(ctx, "insert into tbl1 (a, b) values (?, ?)", 1, "a value").Err(); err != nil {
		t.Fatal("inserting values", err)
	}

	var a int
	if err := ctn.Exec(ctx, "select a from tbl1 where b = ?", "a value").ScanOne(&a); err != nil {
		t.Fatal("reading a back", err)
	}

	if a != 1 {
		t.Errorf("invalid value back: want 1, got %d", a)
	}
}

func TestReleaseOnError(t *testing.T) {
	if st := pprof.Lookup(statementsProfiles.Name()).Count(); st != 0 {
		pprof.Lookup(statementsProfiles.Name()).WriteTo(os.Stderr, 1)
		t.Fatal("another test did not clean up")
	}

	ctn, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := ctn.Exec(ctx, "create table tbl1 (a primary key, b)").Err(); err != nil {
		t.Fatal("cannot create table", err)
	}

	var a string
	err = ctn.Exec(ctx, "select a from tbl1").ScanOne(&a)
	if !errors.Is(err, io.EOF) {
		t.Fatal("error selecting missing entry", err)
	}

	if st := pprof.Lookup(statementsProfiles.Name()).Count(); st != 0 {
		t.Error("statement retained after error", st)
	}
}

func TestPool(t *testing.T) {
	pool, err := OpenPool(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := pool.Exec(ctx, "create table t (a)").Err(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		i := i
		go func() {
			ctx, err := pool.Savepoint(context.Background())
			if err != nil {
				t.Errorf("i=%d err=%s", i, err)
				wg.Done()
				return
			}
			t.Logf("i=%d connection=%p", i, ctx.Value(ckey{}).(*Conn))

			pool.Exec(ctx, "insert into t(a) values (?) ", strconv.Itoa(i))
			var res int
			if err := pool.Exec(ctx, "select count(*) from t").ScanOne(&res); err != nil {
				t.Errorf("i=%d err=%s", i, err)
			}
			t.Logf("i=%d res=%d", i, res)
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond / 10) // force a goroutine switch
			if err := pool.Release(ctx); err != nil {
				t.Errorf("i=%d err=%s", i, err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	var res int
	if err := pool.Exec(ctx, "select count(*) from t").ScanOne(&res); err != nil {
		t.Fatal(err)
	}
	if res != 5 {
		t.Errorf("parallel insert: got %d res", res)
	}
}

func TestRollback(t *testing.T) {
	pool, err := OpenPool(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := pool.Exec(ctx, "create table t (a)").Err(); err != nil {
		t.Fatal(err)
	}

	ctx, err = pool.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		ctx, err := pool.Savepoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.Exec(ctx, "insert into t (a) values (?)", "hello world").Err(); err != nil {
			t.Fatal(err)
		}
		pool.Release(ctx)
		pool.Rollback(ctx) // ignored since release happened before
		var v string
		if err := pool.Exec(ctx, "select a from t").ScanOne(&v); err != nil {
			t.Fatal(err)
		}
		if v != "hello world" {
			t.Error("read back value from nested savepoint", v)
		}
	}

	// top-level nesting can invalidate everything
	pool.Rollback(ctx)
	var w int
	if err := pool.Exec(ctx, "select count(a) from t").ScanOne(&w); err != nil {
		t.Fatal(err)
	}
	if w != 0 {
		t.Error("read back value from top-level savepoint", w)
	}
}

func TestCancel(t *testing.T) {
	t.Skip("too coarse for now")
	db, err := OpenPool(t.TempDir()+"/db",
		RegisterFunc("sleep", func(delay int) int { time.Sleep(time.Duration(delay) * time.Millisecond); return delay }))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	var delay int
	err = db.Exec(ctx, "select sleep(200)").ScanOne(delay)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got invalid result: delay=%d error=%s", delay, err)
	}
}

func TestGenericAccess(t *testing.T) {
	ctn, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := ctn.Exec(ctx, "create table tbl1 (a primary key, b)").Err(); err != nil {
		t.Fatal("cannot create table", err)
	}
	if err := ctn.Exec(ctx, "insert into tbl1 (a, b) values (1, 2), (3, 4)").Err(); err != nil {
		t.Fatal("cannot create table", err)
	}

	ptrs := []string{"1", "2", "3", "4"}
	want := []ColStrings{
		{"a": &ptrs[0], "b": &ptrs[1]},
		{"a": &ptrs[2], "b": &ptrs[3]},
	}

	var got []ColStrings
	st := ctn.Exec(ctx, "select a, b from tbl1")
	for st.Next() {
		cs := make(ColStrings)
		st.Scan(cs)
		got = append(got, cs)
	}

	if st.Err() != nil {
		t.Fatal(st.Err())
	}

	if !cmp.Equal(got, want) {
		t.Error(cmp.Diff(got, want))
	}
}

func BenchmarkLoopTables(bench *testing.B) {
	db, err := OpenPool(bench.TempDir() + "/db")
	if err != nil {
		bench.Fatal(err)
	}
	ctx := context.Background()
	ctx, err = db.Savepoint(ctx)
	if err != nil {
		bench.Fatal(err)
	}
	db.mustExec(ctx, bench, "create table tbl1 (a primary key)")
	db.mustExec(ctx, bench, "create table tbl2 (a, b)")
	for i := 0; i < 20; i++ {
		a := "a_" + strconv.Itoa(i)
		db.mustExec(ctx, bench, "insert into tbl1(a) values (?)", a)
		for i := 0; i < 2000; i++ {
			db.mustExec(ctx, bench, "insert into tbl2(a, b) values(?, ?)", a, "b_"+strconv.Itoa(i))
		}
	}
	if err := db.Release(ctx); err != nil {
		bench.Fatal(err)
	}
	bench.ResetTimer()

	for i := 0; i < bench.N; i++ {
		ctx, err := db.Savepoint(ctx)
		if err != nil {
			bench.Fatal(err)
		}
		st := db.Exec(ctx, "select a from tbl1")
		var a, b string
		for st.Next() {
			st.Scan(&a)
			tt := db.Exec(ctx, "select b from tbl2 where a = ?", a)

			match := 0
			for tt.Next() {
				tt.Scan(&b)
				if len(b) > 0 {
					match++
				}
			}

			if err := tt.Err(); err != nil {
				bench.Fatalf("error scanning %s", err)
			}
			if match != 2000 {
				bench.Errorf("invalid number of match for %s: %d", a, match)
			}
		}
		if st.Err() != nil {
			bench.Fatal(st.Err())
		}
		if err := db.Release(ctx); err != nil {
			bench.Fatal(err)
		}
	}
}

func (db *Connections) mustExec(ctx context.Context, r interface{ Fatal(...any) }, cmd string, args ...any) {
	if err := db.Exec(ctx, cmd, args...).Err(); err != nil {
		r.Fatal(err)
	}
}
