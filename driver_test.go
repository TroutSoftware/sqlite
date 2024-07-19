package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"reflect"
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

	ctn.Exec(ctx, "create table tbl1 (a primary key, b)").GuardErr()
	ctn.Exec(ctx, "insert into tbl1 (a, b) values (?, ?)", 1, "a value").GuardErr()

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
		// The ctx on the left here is a new ctx and has a different
		// savepoint stored in it. This is crucial, otherwise we'd lose
		// the previous savepoint.
		ctx, err := pool.Savepoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pool.Exec(ctx, "insert into t (a) values (?)", "hello world").GuardErr()
		pool.Release(ctx)
		pool.Rollback(ctx) // ignored since release happened before
		assertEquals(t, ctx, pool, "select a from t", "hello world")
	}

	// top-level nesting can invalidate everything, the table should be empty
	pool.Rollback(ctx)
	assertEquals(t, ctx, pool, "select count(a) from t", 0)
}

func TestRollbackTo(t *testing.T) {
	pool, err := OpenPool(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := pool.Exec(ctx, "create table t (a)").Err(); err != nil {
		t.Fatal(err)
	}

	txCtx, err := pool.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pool.Exec(txCtx, "insert into t (a) values (?)", "hello world").GuardErr()
	assertEquals(t, txCtx, pool, "select a from t", "hello world")

	// RollbackTo, the changes are discarded, and the transaction is NOT
	// cancelled. Therefore the following VACUUM attempt made from a different
	// goroutine on a different connection should FAIL.
	err = pool.RollbackTo(txCtx)
	if err != nil {
		t.Fatal(err)
	}

	// The table should be empty
	assertEquals(t, txCtx, pool, "select count(a) from t", 0)

	err = goRunVacuum(pool, BusyTransaction)
	if err != nil {
		t.Fatal(err)
	}

	// The next VACUUM attempt should succeed, as the function Release
	// is getting executed here.
	err = pool.Release(txCtx)
	if err != nil {
		t.Fatal(err)
	}

	err = goRunVacuum(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRollbackWithoutRelease(t *testing.T) {
	pool, err := OpenPool(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pool.Exec(ctx, "create table t (a)").GuardErr()

	txCtx, err := pool.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pool.Exec(txCtx, "insert into t (a) values (?)", "hello world").GuardErr()
	assertEquals(t, txCtx, pool, "select a from t", "hello world")

	// Rollback, the changes are not committed, and the transaction is
	// cancelled. Therefore the following VACUUM attempt made from a different
	// goroutine on a different connection should succeed without any issues.
	err = pool.Rollback(txCtx)
	if err != nil {
		t.Fatal(err)
	}

	err = goRunVacuum(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertEquals(t, ctx, pool, "select count(a) from t", 0)
}

// TODO: this should probably go into a script (cf rsc.io/script).
func assertEquals(t *testing.T, ctx context.Context, x interface {
	Exec(ctx context.Context, query string, args ...any) *Rows
}, query string, want ...any) {
	t.Helper()
	got := make([]any, len(want))
	for i := range got {
		got[i] = reflect.New(reflect.TypeOf(want[i])).Interface()
	}
	err := x.Exec(ctx, query).ScanOne(got...)
	if err != nil {
		t.Fatalf("invalid query %s: %s", query, err)
	}
	gotv := make([]any, len(got))
	for i := range gotv {
		gotv[i] = reflect.ValueOf(got[i]).Elem().Interface()
	}
	if !cmp.Equal(gotv, want) {
		t.Fatal(cmp.Diff(gotv, want))
	}
}

func goRunVacuum(pool *Connections, expect error) error {
	errc := make(chan error)

	go func() {
		err := pool.Exec(context.Background(), "VACUUM;").Err()
		if !errors.Is(err, expect) {
			errc <- fmt.Errorf("unexpected error: %w", err)
		}
		close(errc)
	}()

	return <-errc
}

func TestRollbackNestedSavepointsWithoutRelease(t *testing.T) {
	pool, err := OpenPool(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := pool.Exec(ctx, "create table t (a)").Err(); err != nil {
		t.Fatal(err)
	}

	txCtx1, err := pool.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pool.Exec(txCtx1, "insert into t (a) values (?)", "hello world").GuardErr()
	assertEquals(t, txCtx1, pool, "select a from t", "hello world")

	{
		// The new savepoint is stored in a different context.
		txCtx2, err := pool.Savepoint(txCtx1)
		if err != nil {
			t.Fatal(err)
		}

		pool.Exec(txCtx2, "insert into t (a) values (?)", "hi there").GuardErr()
		assertEquals(t, txCtx2, pool, "select count(a) from t", 2)

		// The inner Rollback, the changes are not committed, and the transaction
		// (the implicit one that was created when the first savepoint was
		// introduced) is not affected. So the VACUUM attempt below should FAIL.
		err = pool.Rollback(txCtx2)
		if err != nil {
			t.Fatal(err)
		}

		// The table should now contain only one entry, because of the rollback
		assertEquals(t, txCtx2, pool, "select count(a) from t", 1)

		err = goRunVacuum(pool, BusyTransaction)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Outer Rollback, the changes are not committed, and implicit transaction
	// is cancelled. So the following VACUUM attempt should succeed.
	err = pool.Rollback(txCtx1)
	if err != nil {
		t.Fatal(err)
	}

	err = goRunVacuum(pool, nil)
	if err != nil {
		t.Fatal(err)
	}

	assertEquals(t, txCtx1, pool, "select count(a) from t", 0)
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

// SQLite type system is dynamic, and the library exposes this fact by using the destination type.
// The tests cases are all slightly different to make sure type inference work as expected.
//
// https://www.sqlite.org/datatype3.html
func TestSerializeAny(t *testing.T) {
	t.Run("bintobin", func(t *testing.T) {
		cases := []struct {
			in  []byte
			out []byte
		}{
			{[]byte{110, 98, 97, 114, 47, 112, 108, 97, 121, 98, 111, 111}, []byte{110, 98, 97, 114, 47, 112, 108, 97, 121, 98, 111, 111}},
		}

		for _, c := range cases {
			cmpValues(t, c.in, c.out)
		}
	})

	t.Run("bintostring", func(t *testing.T) {
		cases := []struct {
			in  []byte
			out string
		}{
			{[]byte{110, 98, 97, 114, 47, 112, 108, 97, 121, 98, 111, 111}, "nbar/playboo"},
		}

		for _, c := range cases {
			cmpValues(t, c.in, c.out)
		}
	})

	t.Run("inttobool", func(t *testing.T) {
		cases := []struct {
			in  int
			out bool
		}{
			{-1, false},
			{0, false},
			{1, true},
			{2, true},
		}

		for _, c := range cases {
			cmpValues(t, c.in, c.out)
		}
	})
}

func cmpValues[T, U any](t *testing.T, in T, out U) {
	ctn, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := ctn.Exec(ctx, "create table tbl1 (a)").Err(); err != nil {
		t.Fatal("creating table", err)
	}

	if err := ctn.Exec(ctx, "insert into tbl1 (a) values (?)", in).Err(); err != nil {
		t.Fatal("inserting value", err)
	}

	var got U
	if err := ctn.Exec(ctx, "select a from tbl1").ScanOne(&got); err != nil {
		t.Fatal("reading value", err)
	}

	if !cmp.Equal(got, out) {
		t.Error(cmp.Diff(got, out))
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

func TestMultiValueScansAreIgnored(t *testing.T) {
	conn, err := Open(t.TempDir() + "/database.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), "create table if not exists test (a text, b text)").Err(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), "insert into test (a, b) VALUES(?,?)", plainstring("dog:a"), plainstring("cat:b")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), "insert into test (a, b) VALUES(?,?)", plainstring("fff:a"), plainstring("mmm:b")).Err(); err != nil {
		t.Fatal(err)
	}
	var as, bs []plainstring
	rows := conn.Exec(context.Background(), "select a, b from test order by a")
	for rows.Next() {
		var a, b plainstring
		rows.Scan(&a, &b)
		as = append(as, a)
		bs = append(bs, b)
	}
	if !cmp.Equal(as, plsg("dog:a", "fff:a")) {
		t.Error("as differ", cmp.Diff(as, plsg("dog:a", "fff:a")))
	}

	if !cmp.Equal(bs, plsg("cat:b", "mmm:b")) {
		t.Error("bs differ", cmp.Diff(bs, plsg("cat:b", "mmm:b")))
	}

}

type plainstring string

func plsg(ss ...plainstring) []plainstring { return ss }

func (r plainstring) MarshalBinary() ([]byte, error) {
	return []byte(string(r)), nil
}
func (r *plainstring) UnmarshalBinary(dt []byte) error {
	*r = plainstring(string(dt))
	return nil
}

// test helpers when OK to panic
func (r *Rows) GuardErr() {
	if err := r.Err(); err != nil {
		panic(err)
	}
}
