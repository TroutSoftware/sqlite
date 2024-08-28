// Package sqlite implements a driver over the SQLite database engine.
//
// To get started, the [Open] and [Conn.Exec] methods should be sufficient,
// but do note that [OpenPool] is used throughout the server code.
//
// The package API is designed to expose as possible of the underlying engine as possible,
// including the option to expose functions and data structures via [Register].
package sqlite

// #include <amalgamation/sqlite3.h>
// #include <stdint.h>
//
// extern char * go_strcpy(_GoString_ st);
// extern void go_free(void*);
// extern int sqlite_BindGoPointer(sqlite3_stmt* stmt, int pos, uintptr_t ptr, const char* name);
import "C"

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"reflect"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

// SQLITE3 wraps a C pointer type for export.
// See [Register] for information about use.
type SQLITE3 *C.sqlite3

// Register adds a new statically compiled extension to register when a new SQLite connection is opened.
// It should be called in a sub-package init function.
//
// Most of the time, f is going to be returned from [RegisterTable] or [RegisterFunc].
//
// If not, due to the way CGO handles names [bug-13467], callers need to wrap this in an unsafe pointer:
//
//	(*C.sqlite3)(unsafe.Pointer(db))
//
// [bug-13467]: https://github.com/golang/go/issues/13467
func Register(f ...func(SQLITE3)) { vtables = append(vtables, f...) }

// vtables is a list of all compiled extensions that will be registered when a new SQLite connection is opened
var vtables []func(SQLITE3)

// LoadExtensions loads all registered extensions against the database db.
// This function is automatically called when [Open] is, and is made available only for modules compiled as a shared library.
func LoadExtensions(db SQLITE3) {
	for _, f := range vtables {
		f(db)
	}
}

var errText = map[C.int]error{
	C.SQLITE_ERROR:         errors.New("SQL error or missing database"),
	C.SQLITE_INTERNAL:      errors.New("internal logic error in SQLite"),
	C.SQLITE_PERM:          errors.New("access permission denied"),
	C.SQLITE_ABORT:         errors.New("callback routine requested an abort"),
	C.SQLITE_BUSY:          errors.New("the database file is busy"),
	C.SQLITE_LOCKED:        errors.New("a table in the database is locked"),
	C.SQLITE_NOMEM:         errors.New("a malloc() failed"),
	C.SQLITE_READONLY:      errors.New("attempt to write a readonly database"),
	C.SQLITE_INTERRUPT:     context.DeadlineExceeded,
	C.SQLITE_IOERR:         errors.New("some kind of disk I/O error occurred"),
	11:                     errors.New("the database disk image is malformed"),
	12:                     errors.New("NOT USED. Table or record not found"),
	13:                     errors.New("insertion failed because database is full"),
	14:                     errors.New("unable to open the database file"),
	15:                     errors.New("NOT USED. Database lock protocol error"),
	16:                     errors.New("Database is empty"),
	C.SQLITE_SCHEMA:        errors.New("the database schema changed"),
	C.SQLITE_TOOBIG:        errors.New("string or BLOB exceeds size limit"),
	C.SQLITE_CONSTRAINT:    errors.New("abort due to constraint violation"),
	C.SQLITE_MISMATCH:      errors.New("data type mismatch"),
	21:                     errors.New("library used incorrectly"),
	22:                     errors.New("ases OS features not supported on host"),
	23:                     errors.New("authorization denied"),
	24:                     errors.New("auxiliary database format error"),
	25:                     errors.New("2nd parameter to sqlite3_bind out of range"),
	26:                     errors.New("file opened that is not a database file"),
	C.SQLITE_ROW:           errors.New("sqlite3_step() has another row ready"),
	C.SQLITE_DONE:          errors.New("sqlite3_step() has finished executing"),
	C.SQLITE_BUSY_SNAPSHOT: errors.New("snapshot is busy"),
}

// Open creates a new database connection stored at name, and load exts.
func Open(name string, exts ...func(SQLITE3)) (*Conn, error) {
	if C.sqlite3_threadsafe() == 0 {
		return nil, errors.New("sqlite library was not compiled for thread-safe operation")
	}

	var db *C.sqlite3
	cname := C.go_strcpy(name)
	defer C.go_free(unsafe.Pointer(cname))
	rv := C.sqlite3_open_v2(cname, &db,
		C.SQLITE_OPEN_FULLMUTEX|
			C.SQLITE_OPEN_READWRITE|
			C.SQLITE_OPEN_CREATE|
			C.SQLITE_OPEN_WAL|
			C.SQLITE_OPEN_URI,
		nil)
	if rv != C.SQLITE_OK {
		return nil, errText[rv]
	}

	C.sqlite3_extended_result_codes(db, 1)
	C.sqlite3_busy_timeout(db, 10_000)

	for _, f := range append(vtables, exts...) {
		f(db)
	}

	return &Conn{db: db}, nil
}

// Conn is a connection to a given database.
// Conn is safe for concurrent use.
//
// Internally, Conn maps to an sqlite3 object,
// and present the same characteristics when not documented otherwise.
type Conn struct {
	// layout warning:
	// the xUpdate implementation expects that creating a connection by simpling filling in the db pointer is correct.
	// alterations to the code should preserve this property.

	db   *C.sqlite3
	mxdb sync.Mutex // protect call to errmsg.
	// take lock before calling the SQLite statements operator
	// release on the happy path
	// pass with lock held to the error method, which will release it

	zombie *time.Timer
	next   *Conn // for free list
}

func (c *Conn) error(rv C.int) error {
	defer c.mxdb.Unlock()
	switch rv {
	case C.SQLITE_INTERRUPT:
		return context.DeadlineExceeded
	case C.SQLITE_OK:
		return nil
	case C.SQLITE_MISUSE:
		panic(fmt.Errorf("%s: %w", errText[rv], errorString{s: C.GoString(C.sqlite3_errmsg(c.db))}))
	case C.SQLITE_BUSY, C.SQLITE_BUSY_SNAPSHOT:
		return BusyTransaction // the entire transaction must be retried
	}

	return fmt.Errorf("%s: %w", errText[rv], errorString{s: C.GoString(C.sqlite3_errmsg(c.db))})
}

// shallow error wrapper, allowing quick comparison with standard Go
type errorString struct{ s string }

func (e errorString) Error() string { return e.s }
func (e errorString) Is(target error) bool {
	if target == e {
		return true
	}
	// match errors.New, fmt
	if target.Error() == e.s {
		return true
	}
	return false
}

// Exec executes cmd, optionally binding args to parameters in the query.
// Rows are a a cursor over the results, and the first row is already executed: commands needs not call [Rows.Next] afterwards.
// Arguments are matched by position.
//
// This function will panic if the command is invalid SQL.
// This is intented for static SQL statements (written in your code), not for untrusted SQL.
func (c *Conn) Exec(ctx context.Context, cmd string, args ...any) *Rows {
	cmdstr := C.go_strcpy(cmd)
	defer C.go_free(unsafe.Pointer(cmdstr))
	var tail *C.char
	var s *C.sqlite3_stmt

	c.mxdb.Lock()
	rv := C.sqlite3_prepare_v2(c.db, cmdstr, C.int(len(cmd)+1), &s, &tail)
	if rv != C.SQLITE_OK {
		return &Rows{err: c.error(rv), stmt: &stmt{c: c}}
	}
	c.mxdb.Unlock()

	if C.GoString(tail) != "" {
		return &Rows{err: fmt.Errorf("multiple statements in call to exec (left %s)", C.GoString(tail)), stmt: &stmt{c: c}}
	}
	st := &stmt{c: c, s: s}
	statementsProfiles.Add(st, 3)
	if err := st.start(args); err != nil {
		return &Rows{err: err, stmt: st}
	}

	return &Rows{ctx: ctx, stmt: st, err: st.step(ctx), scflag: true}
}

func (c *Conn) Close() error {
	rv := C.sqlite3_close_v2(c.db)
	if rv != C.SQLITE_OK {
		return errText[rv]
	}
	return nil
}

var statementsProfiles = pprof.NewProfile("t.sftw/sqlite/statements")

// Rows are iterator structure over the underlying database statement results.
//
// Rows are lightweigth object, and should not be reused after [Rows.Err] or [Rows.ScanOne] calls.
type Rows struct {
	err error
	ctx context.Context

	scflag bool
	stmt   *stmt
	final  func()
}

// NewErroredRows create a cursor that will fail immediatly.
// This is useful for API that pipeline connection creation and direct query.
func NewErroredRows(ctn *Conn, err error) *Rows {
	return &Rows{stmt: &stmt{c: ctn}, err: err}
}

// NumColumn returns the count of columns returned by the current statement.
// Use [Rows.ColumnName] if the name is useful.
func (rows *Rows) NumColumn() int { return int(C.sqlite3_column_count(rows.stmt.s)) }

// ColumnName return the ith (zero-based) column name.
// This is mostly convenient in a loop:
//
//	for i := 0; i < st.NumColumn(); i++ {
//		buf.WriteString(st.ColumnName(i) + "\t")
//	}
func (rows *Rows) ColumnName(i int) string {
	return C.GoString(C.sqlite3_column_name(rows.stmt.s, C.int(i)))
}

// Next advances the cursor to the next result in the set.
// It returns false if there are no more results, or if an error, or a timeout occur.
// Use [Rows.Err] to disambiguate between those cases.
func (rows *Rows) Next() bool {
	defer trace.StartRegion(rows.ctx, "sqlite_row_next").End()

	switch {
	case rows.err != nil:
		return false
	case rows.scflag:
		rows.scflag = false
		return true
	}

	// note: step must run in the main goroutine, since the library ensures that calls are locked to an OS thread
	rows.err = rows.stmt.step(rows.ctx)
	return rows.err == nil
}

// Scan unmarshals the underlying SQLite value into a Go value.
// Values in dst are matched by position against the columns in the query, e.g.
//
//	 rows := ctn.Exec(ctx, "select a, b from t")
//	 for rows.Next() {
//		var c, d string
//		rows.Scan(&c, &d)
//		// c -> column a
//		// d -> column b
//	}
//
// Scan defers errors to the [Rows.Err] method (but note that [Rows.Next] will stop at the first error).
//
// Conversion is done depending on the SQLite [type affinity] and the type in Go.
// Richer Go types (e.g. [bytes.Buffer], or [time.Time]) are automatically read too.
// Read the code for the full map – but if you’re relying on this, you are probably already doing something too smart.
//
// [type affinity]: https://www.sqlite.org/datatype3.html
func (rows *Rows) Scan(dst ...any) {
	defer trace.StartRegion(rows.ctx, "sqlite_row_scan").End()

	if rows.err != nil {
		return
	}

	rows.scflag = false
	if len(dst) == 1 {
		switch orig := dst[0].(type) {
		case *MultiString:
			cnt := C.sqlite3_column_count(rows.stmt.s)
			*orig = make(MultiString, cnt)
			dst = make([]any, cnt)
			for i := range *orig {
				dst[i] = &(*orig)[i]
			}
		case ColStrings:
			cnt := C.sqlite3_column_count(rows.stmt.s)
			dst = make([]any, cnt)
			for i := 0; i < int(cnt); i++ {
				cn := C.GoString(C.sqlite3_column_name(rows.stmt.s, C.int(i)))
				v := new(string)
				orig[cn] = v
				dst[i] = v
			}
		}
	}

	// note: scan must run in the main goroutine, since the library ensures that calls are locked to an OS thread
	rows.err = rows.stmt.scan(dst)
}

// MultiString is used to read all values of a statement as string.
// This is useful if you don’t know ahead of time the values returned.
type MultiString []string

// Err finalizes the statement, and return an error if any.
// [Rows] should not be used after this.
func (r *Rows) Err() error {
	r.stmt.finalize()
	statementsProfiles.Remove(r.stmt)

	if r.final != nil {
		r.final()
		r.final = nil // prevent double free
	}

	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

// ColStrings augments [MultiString] with the name of the columns.
// the pointer type is required to comply with Go’s addressability constraints on maps.
type ColStrings map[string]*string

var DuplicateRecords = errors.New("duplicate records in scanone")
var BusyTransaction = errText[C.SQLITE_BUSY]

// ScanOne is a convenient shortcut over [Rows.Scan], returning the first value.
// [DuplicateRecords] will be returned if more than one record match.
func (r *Rows) ScanOne(dst ...any) error {
	if r.err != nil {
		r.Err() // execute for freeing side-effects
		return r.err
	}

	r.Scan(dst...)
	// should have read it all
	if r.Next() {
		r.err = DuplicateRecords
	}
	return r.Err()
}

var bpool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// GetBuffer returns a bytes buffer from a system-wide pool.
// It is useful to avoid too much allocation when serializing.
// The buffer must be release with [ReturnBuffer].
func GetBuffer() *bytes.Buffer { return bpool.Get().(*bytes.Buffer) }

// ReturnBuffer returns the buffer to the pool.
// The buffer must not be used afterwards (this also mean the bytes returned from [bytes.buffer.Bytes]).
func ReturnBuffer(b *bytes.Buffer) {
	b.Reset()
	bpool.Put(b)
}

type stmt struct {
	c   *Conn
	s   *C.sqlite3_stmt
	err error
}

func (s *stmt) start(args []any) error {
	n := int(C.sqlite3_bind_parameter_count(s.s))
	if n != len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), n)
	}

	s.c.mxdb.Lock()
	for i, v := range args {
		var rv C.int
		switch v := v.(type) {
		case nil:
			rv = C.sqlite3_bind_null(s.s, C.int(i+1))
		case float64:
			rv = C.sqlite3_bind_double(s.s, C.int(i+1), C.double(v))
		case int64:
			rv = C.sqlite3_bind_int64(s.s, C.int(i+1), C.sqlite3_int64(v))
		case int:
			rv = C.sqlite3_bind_int64(s.s, C.int(i+1), C.sqlite3_int64(v))

		case bool:
			var vi int64
			if v {
				vi = 1
			}
			rv = C.sqlite3_bind_int64(s.s, C.int(i+1), C.sqlite3_int64(vi))

		case time.Time:
			cstr := C.go_strcpy(v.UTC().Format(timefmt[0]))
			rv = C.sqlite3_bind_text(s.s, C.int(i+1), cstr, -1, (*[0]byte)(C.sqlite3_free))

		case string:
			cstr := C.go_strcpy(v)
			rv = C.sqlite3_bind_text(s.s, C.int(i+1), cstr, -1, (*[0]byte)(C.sqlite3_free))

		case *C.sqlite3_stmt:
			// STATIC since this is only used in [Rows.Bytecode] (C pointers are not available out of the function)
			rv = C.sqlite3_bind_pointer(s.s, C.int(i+1), unsafe.Pointer(v), sqlite3StatementPointerType, C.SQLITE_STATIC)

		case PointerValue:
			rv = C.sqlite_BindGoPointer(s.s, C.int(i+1), C.uintptr_t(allocHandle(v.v)), namefor(reflect.TypeOf(v.v).Elem()))

		case []byte:
			rv = C.sqlite3_bind_blob(s.s, C.int(i+1), unsafe.Pointer(unsafe.SliceData(v)), C.int(len(v)), C.SQLITE_TRANSIENT)

		case *bytes.Buffer:
			vv := v.Bytes()
			rv = C.sqlite3_bind_blob(s.s, C.int(i+1), unsafe.Pointer(unsafe.SliceData(vv)), C.int(len(vv)), C.SQLITE_TRANSIENT)

		default:
			bm, ok := v.(encoding.BinaryMarshaler)
			if !ok {
				return fmt.Errorf("%T not a base type, must implement BinaryMarshaller", v)
			}
			dt, err := bm.MarshalBinary()
			if err != nil {
				return err
			}

			rv = C.sqlite3_bind_blob(s.s, C.int(i+1), unsafe.Pointer(unsafe.SliceData(dt)), C.int(len(dt)), C.SQLITE_TRANSIENT)
		}

		if rv != C.SQLITE_OK {
			return s.c.error(rv)
		}

	}
	s.c.mxdb.Unlock()
	return nil
}

var sqlite3StatementPointerType = C.go_strcpy("stmt-pointer")

func (s *stmt) interrupt() { C.sqlite3_interrupt(s.c.db) }
func (s *stmt) finalize() {
	if s.s != nil {
		C.sqlite3_finalize(s.s)
		s.s = nil
	}
}

const maxslice = 1<<31 - 1

var timefmt = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

func (s *stmt) step(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			// s.interrupt()
		}
	}()
	defer close(done)

	s.c.mxdb.Lock()
	rv := C.sqlite3_step(s.s)
	const max_retry = 5
	for i := 0; i < max_retry; i++ {
		switch rv {
		case C.SQLITE_ROW:
			s.c.mxdb.Unlock()
			return nil
		case C.SQLITE_DONE:
			s.c.mxdb.Unlock()
			return io.EOF
		case C.SQLITE_OK:
			return s.c.error(C.SQLITE_MISUSE)
		default:
			return s.c.error(rv)
		}
	}

	return s.c.error(rv)
}

func (stmt *stmt) scan(dst []any) error {
	// double type switch, to match SQLite column affinity
	for i := range dst {
		switch typ := C.sqlite3_column_type(stmt.s, C.int(i)); typ {
		default:
			return fmt.Errorf("unexpected sqlite3 column type %d", typ)
		case C.SQLITE_INTEGER:
			val := int64(C.sqlite3_column_int64(stmt.s, C.int(i)))

			switch v := dst[i].(type) {
			case *time.Time:
				*v = time.Unix(val, 0).UTC()
			case *bool:
				*v = val > 0
			case *int:
				*v = int(val)
			case *string:
				*v = strconv.FormatInt(val, 10)
			}

		case C.SQLITE_FLOAT:
			dst[i] = float64(C.sqlite3_column_double(stmt.s, C.int(i)))

		case C.SQLITE_BLOB, C.SQLITE_TEXT:
			n := int(C.sqlite3_column_bytes(stmt.s, C.int(i)))
			var b []byte
			if n > 0 {
				b = unsafe.Slice((*byte)(C.sqlite3_column_blob(stmt.s, C.int(i))), n)
			}

			switch v := dst[i].(type) {
			case *int:
				vv, err := strconv.ParseInt(string(b), 10, 64)
				if err != nil {
					return err
				}
				*v = int(vv)
			case *[]byte:
				if cap(*v) < len(b) {
					*v = make([]byte, len(b))
				} else {
					*v = (*v)[:len(b)]
				}
				copy(*v, b)
			case *bytes.Buffer:
				v.Write(b)
			case *string:
				*v = string(b)
			case *time.Time:
				s := string(b)
				for _, f := range timefmt {
					if t, err := time.Parse(f, s); err == nil {
						*v = t
						break
					}
				}
			case *PointerValue:
				panic("Pointer values cannot be scanned")
			default:
				unm, ok := dst[i].(encoding.BinaryUnmarshaler)
				if !ok {
					panic(fmt.Sprintf("%T invalid decoder type", v))
				}
				if err := unm.UnmarshalBinary(b); err != nil {
					return err
				}
			}

		case C.SQLITE_NULL:
			dst[i] = nil
		}
	}
	return nil
}

// Savepoint creates a new [savepoint] in transaction (think about begin).
// It is the responsibility of the caller to [Conn.Release] it.
//
// Most of the time, the implementation of [Pool.Savepoint] should be preferred.
//
// [savepoint]: https://sqlite.org/lang_savepoint.html
func (ctn *Conn) Savepoint(ctx context.Context) (context.Context, error) {
	spn := randname()
	sp := &savepoint{name: spn}

	err := ctn.Exec(ctx, "SAVEPOINT "+spn).Err()
	return context.WithValue(ctx, spkey{}, sp), err
}

func randname() string {
	nm := make([]byte, 4)
	rand.Read(nm)
	val := uint32(nm[0])<<24 | uint32(nm[1])<<16 | uint32(nm[2])<<8 | uint32(nm[3])

	var buf [10]byte // big enough for 32bit value base 10
	i := len(buf) - 1
	for val >= 10 {
		q, r := bits.Div32(0, val, 10)
		buf[i] = byte('a' + r)
		i--
		val = q
	}
	// val < 10
	buf[i] = byte('a' + val)
	return string(buf[i:])

}

// Release returns the given savepoint.
// It is safe to call this after Rollback
func (ctn *Conn) Release(ctx context.Context) error {
	sp := ctx.Value(spkey{}).(*savepoint)
	sp.released = true
	return ctn.Exec(ctx, "RELEASE "+sp.name).Err()
}

// RollbackTo rolls back all changes to the current changepoint, but it does
// not release the existing savepoint. This can be useful for retries, as it
// restart any potential implicit transaction that's related to this
// savepoint.
func (ctn *Conn) RollbackTo(ctx context.Context) error {
	sp := ctx.Value(spkey{}).(*savepoint)
	return ctn.Exec(ctx, "ROLLBACK TO "+sp.name).Err()
}

// Rollback restores the DB with the original state that the savepoint had
// captured and it releases the savepoint not to leave any zombie transactions
// behind. If the savepoint has already been 'Release'd, this function returns
// immediately without any errors.
// Rollback SHOULD be called with a defer statement for EVERY successfully
// created savepoint, right after its creation.
func (ctn *Conn) Rollback(ctx context.Context) error {
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
	}

	return err
}
