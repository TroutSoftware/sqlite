package sqlite

/*
#include <amalgamation/sqlite3.h>
#include <stdint.h>
#include <string.h>

extern int register_new_vtable(
    sqlite3 *db,
    const char *name,
    uintptr_t go_handle,
    char **pzErrMsg);

extern char * go_strcpy(_GoString_ st);
extern void go_free(void*);
extern void sqlite_ResultGoPointer(sqlite3_context* ctx, uintptr_t ptr, const char* name);
*/
import "C"

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"hash/maphash"
	"reflect"
	"runtime"
	"runtime/cgo"
	"strings"
	"unsafe"
)

var extensions = expvar.NewMap("t.sftw/sqlite/extensions")

// RegisterTable creates a virtual table at name.
// The virtual table definition is read from T, see [Filtrer] for more details.
func RegisterTable[T Filtrer[T]](name string, vt T, opts ...VirtualTableOption) func(SQLITE3) {
	return func(db SQLITE3) {
		vm := buildReqVM(reflect.TypeOf(vt))
		extensions.Add(name, 1)
		if ix, ok := any(vt).(Indexer); ok {
			vm.bestIndex = ix.BestIndex
		} else {
			vm.bestIndex = vm.defaultBestIndex
		}
		vm.filter = func(n int, cs Constraints) Iter[any] {
			return iterAny[T]{underlying: vt.Filter(n, cs)}
		}
		if up, ok := any(vt).(Updater[T]); ok {
			vm.update = func(ctx context.Context, i int64, v any) error { return up.Update(ctx, i, v.(T)) }
			vm.rtype = reflect.TypeOf(vt)
		}

		if h, ok := reflect.TypeOf(vt).MethodByName("Hash64"); ok {
			vm.rowid = func(v any) int64 {
				// func with receiver as first argument
				params := []reflect.Value{reflect.ValueOf(v)}
				results := h.Func.Call(params)
				return results[0].Interface().(int64)
			}
		} else {
			vm.rowid = func(_ any) int64 { return -1 }
		}

		for _, o := range opts {
			o(&vm)
		}

		// install top-level placeholder for functions
		for _, fn := range vm.overloads {
			cn := C.go_strcpy(fn.name)
			rc := C.sqlite3_overload_function((*C.sqlite3)(unsafe.Pointer(db)), cn, C.int(fn.nargs))
			if rc != C.SQLITE_OK {
				panic(fmt.Sprintf("cannot overload function %s: %d", fn.name, rc))
			}
			C.go_free(unsafe.Pointer(cn))
		}

		C.register_new_vtable((*C.sqlite3)(unsafe.Pointer(db)),
			C.go_strcpy(name), // C-side will free
			C.uintptr_t(cgo.NewHandle(vm)),
			nil)
	}
}

// VirtualTableOption allows customisation of the virtual table behavior.
type VirtualTableOption func(*rVM)

// OverloadFunc permits [function overloading] in a virtual table.
//
// If the function takes two arguments, and returns either a boolean, or a boolean or an error, it will be used as a hint to filtering.
// In this case, the operation in the constraint will be equal to ConstraintFunc + index, with index the position in the overloaded functions (_including_ non-indexing ones).
//
// [function overloading]: https://sqlite.org/vtab.html#xfindfunction
func OverloadFunc(name string, fn any) VirtualTableOption {
	const sqliteRHS = 1 // second argument zero based

	return func(vm *rVM) {
		tt := reflect.TypeOf(fn)
		call := ocall{name: name, nargs: tt.NumIn()}
		if tt.Kind() != reflect.Func {
			panic("OverloadFunc only accepts functions")
		}

		if tt.NumOut() == 2 && tt.Out(1) != errType {
			panic("two-arguments functions must return an error as the second value")
		}

		if tt.NumIn() == 2 && (tt.NumOut() == 1 || tt.NumOut() == 2) && tt.Out(0) == boolType {
			call.ixer = true
			if tt.In(sqliteRHS).Kind() == reflect.Pointer {
				call.pn = namefor(tt.In(sqliteRHS).Elem())
			}
		}

		call.cm = cgo.NewHandle(reflectCallMachine(fn))
		vm.overloads = append(vm.overloads, call)
	}
}

var cseed = maphash.MakeSeed()

// RegisterConstants creates a virtual table which always returns the same value
func RegisterConstant(name string, value any) func(SQLITE3) {
	return func(db SQLITE3) {
		vm := buildReqVM(reflect.TypeOf(value))
		vm.bestIndex = vm.defaultBestIndex
		vm.filter = func(n int, cs Constraints) Iter[any] {
			return FromOne(value)
		}
		vm.rowid = func(_ any) int64 { return int64(maphash.String(cseed, name)) }
		C.register_new_vtable((*C.sqlite3)(unsafe.Pointer(db)),
			C.go_strcpy(name), // C-side will free
			C.uintptr_t(cgo.NewHandle(vm)),
			nil)
	}
}

// bit of a hack: filter always call next after the first result is done
type cstResource struct{ v any }

func (r *cstResource) Filter(int, Constraints) Iter[any] { return FromOne(r.v) }

type rVM struct {
	required bitset
	filtered bitset
	ignored  bitset
	hidden   bitset
	nocmp    bitset

	names string
	nidx  []int

	ptypes map[string]*C.char // linked to new bestIndex

	bestIndex func(IndexState, Constraints) (estimatedCost, estimatedRows int64)
	filter    func(int, Constraints) Iter[any]

	update func(context.Context, int64, any) error
	rtype  reflect.Type

	rowid func(any) int64

	overloads []ocall
}

type ocall struct {
	name  string
	ixer  bool
	nargs int
	cm    cgo.Handle
	pn    *C.char
}

func (v rVM) namefor(col int) string {
	return v.names[v.nidx[col]:v.nidx[col+1]]
}

func (vm rVM) defaultBestIndex(st IndexState, cs Constraints) (ecost, erow int64) {
	const estimateFilterEffect = 10
	const estimateGenericBadness = 100_000

	ecost = estimateGenericBadness
	for i, c := range cs {
		if c.Operation == ConstraintEQ && vm.filtered.Test(c.num) {
			ecost /= estimateFilterEffect
			st.Use(i)
		}
	}
	return ecost, 0
}

type bitset uint64

func (s *bitset) Add(i int)            { *s |= bitset(1 << i) }
func (s bitset) Test(i int) bool       { return s&bitset(1<<i) > 0 }
func (s bitset) Include(t bitset) bool { return s&t == t }

type iterAny[T any] struct {
	underlying Iter[T]
}

func (i iterAny[T]) Err() error { return i.underlying.Err() }
func (i iterAny[T]) Next() bool { return i.underlying.Next() }
func (i iterAny[T]) Value() any { return i.underlying.Value() }

func buildReqVM(st reflect.Type) rVM {
	var rvm rVM
	var buf strings.Builder

	if st.NumField() > 63 {
		panic("too many fields in structure, only 64 can be used.\n\n Use json if you need more \n\n")
	}

	rvm.ptypes = make(map[string]*C.char, st.NumField())
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		vtab := f.Tag.Get("vtab")
		vparts := strings.Split(vtab, ",")
		if vparts[0] == "" {
			panic("all virtual table fields must be annotated")
		}

		if vparts[0] == "-" {
			rvm.ignored.Add(i)
			continue // ignore explicit
		}

		for _, vp := range vparts[1:] {
			switch vp {
			case "filtered":
				rvm.filtered.Add(i)
			case "required":
				rvm.required.Add(i)
				rvm.filtered.Add(i)
			case "hidden":
				rvm.hidden.Add(i)
				rvm.filtered.Add(i)
			case "nocmp":
				rvm.nocmp.Add(i)
			default:
				panic("unknown tag " + vp)
			}
		}
		rvm.nidx = append(rvm.nidx, buf.Len())
		buf.WriteString(vparts[0])

		if f.Type.Kind() == reflect.Pointer {
			rvm.ptypes[vparts[0]] = namefor(f.Type.Elem())
			rvm.nocmp.Add(i)
		}
	}
	rvm.nidx = append(rvm.nidx, buf.Len())
	rvm.names = buf.String()

	return rvm
}

//export goGetTableDefinition
func goGetTableDefinition(vtab C.uintptr_t, def **C.char) int {
	vm := cgo.Handle(vtab).Value().(rVM)
	var buf strings.Builder
	buf.WriteString("create table xx(") // SQLite ignores the name
	for i := 0; i < len(vm.nidx)-1; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}

		buf.WriteString("\"" + vm.names[vm.nidx[i]:vm.nidx[i+1]] + "\"")
		if vm.hidden.Test(i) {
			buf.WriteString(" hidden")
		}
	}
	buf.WriteString(")")
	*def = C.go_strcpy(buf.String())
	runtime.KeepAlive(buf)
	return C.SQLITE_OK
}

//export goSetBestIndex
func goSetBestIndex(ht C.uintptr_t, idx *C.sqlite3_index_info) int {
	vm := cgo.Handle(ht).Value().(rVM)
	ci := (*[1 << 10]C.struct_sqlite3_index_constraint)(unsafe.Pointer(idx.aConstraint))
	co := (*[1 << 10]C.struct_sqlite3_index_constraint_usage)(unsafe.Pointer(idx.aConstraintUsage))
	xb := indexbuilder{out: co[:idx.nConstraint]}

	var cs Constraints
	var colused bitset
	for i := 0; i < int(idx.nConstraint); i++ {
		colused.Add(int(ci[i].iColumn))

		if ci[i].usable == 0 {
			// take care of excluding plans that omit a required field
			// cf https://sqlite.org/vtab.html#enforcing_required_parameters_on_table_valued_functions
			if vm.required.Test(int(ci[i].iColumn)) {
				return C.SQLITE_CONSTRAINT
			}

			continue
		}

		if vm.nocmp.Test(int(ci[i].iColumn)) {
			co[i].omit = 1
		}

		if vm.filtered.Test(int(ci[i].iColumn)) {
			cs = append(cs, Constraint{
				num:       int(ci[i].iColumn),
				Column:    vm.namefor(int(ci[i].iColumn)),
				Operation: Op(ci[i].op),
			})
			xb.cmap = append(xb.cmap, i)
		}
	}

	// report missing mandatory fields
	// cf https://sqlite.org/vtab.html#enforcing_required_parameters_on_table_valued_functions
	if !colused.Include(vm.required) {
		return C.SQLITE_ERROR
	}

	xb.cs = cs
	ecost, erow := vm.bestIndex(&xb, cs)
	if ecost != 0 {
		idx.estimatedCost = C.double(ecost)
	}
	if erow != 0 {
		idx.estimatedRows = C.sqlite3_int64(erow)
	}

	xb.buf.WriteByte('\x00')
	idx.idxStr = C.go_strcpy(xb.buf.String())
	idx.needToFreeIdxStr = 1 // go strcpy uses sqlite3 mem management

	return C.SQLITE_OK
}

type indexbuilder struct {
	buf strings.Builder
	ix  int

	argc int
	out  []C.struct_sqlite3_index_constraint_usage
	cs   []Constraint
	cmap []int
}

func (b *indexbuilder) Use(ci int) {
	c := b.cs[ci]
	b.argc++ // must start at one
	b.out[b.cmap[ci]].argvIndex = C.int(b.argc)
	if b.argc > 1 {
		fmt.Fprint(&b.buf, ", ")
	}
	fmt.Fprintf(&b.buf, "%s %s ?", c.Column, c.Operation)
}

func (b *indexbuilder) SelectIndex(n int) { b.ix = n }

//export goAllocCursor
func goAllocCursor(cside C.uintptr_t) C.uintptr_t {
	return C.uintptr_t(allocHandle(reserve{}))
}

//export goFilter
func goFilter(vtab C.uintptr_t, cursr C.uintptr_t, inum C.int, cidx *C.char, argc C.int, argv **C.sqlite3_value, cerr **C.char) C.int {
	vm := cgo.Handle(vtab).Value().(rVM)

	cs := make(Constraints, argc)
	ids := C.GoString(cidx)
	if argc > 0 {
		idx := strings.Split(ids, ", ")
		vp := (*[1 << 10]*C.sqlite3_value)(unsafe.Pointer(argv))

		for i, c := range idx {
			fmt.Sscan(c, &cs[i].Column, &cs[i].Operation)

			var pointerType *C.char
			// with overloaded functions, types cannot be infered from columns, so need to consider call site
			if cs[i].Operation < C.SQLITE_INDEX_CONSTRAINT_FUNCTION {
				if pn, ok := vm.ptypes[cs[i].Column]; ok {
					pointerType = pn
				}
			} else {
				pn := vm.overloads[cs[i].Operation-C.SQLITE_INDEX_CONSTRAINT_FUNCTION].pn
				if pn != nil {
					pointerType = pn
				}
			}
			if pointerType != nil {
				hdl := uintptr(C.sqlite3_value_pointer(vp[i], pointerType))
				if hdl == 0 {
					C.go_free(unsafe.Pointer(*cerr))
					*cerr = C.go_strcpy("cannot read pointer for type " + C.GoString(pointerType) + " via pointer-passing interface")
					return C.SQLITE_MISUSE
				}
				cs[i].Value = accessHandle(hdl)
				continue
			}

			switch C.sqlite3_value_type(vp[i]) {
			case C.SQLITE_INTEGER:
				cs[i].Value = int(C.sqlite3_value_int(vp[i]))
			case C.SQLITE_NULL:
				// nothing to do
			case C.SQLITE_BLOB:
				// a call to sqlite3_value_bytes can invalidate the return of sqlite3_value_blob
				// https://www.sqlite.org/c3ref/value_blob.html
				l := C.sqlite3_value_bytes(vp[i])
				cs[i].Value = C.GoBytes(unsafe.Pointer(C.sqlite3_value_blob(vp[i])), l)
			case C.SQLITE_TEXT:
				// the signed values are irrelevant here, this is valid UTF-8
				// Go ignores it too in runtime/gostring
				cs[i].Value = C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_value_text(vp[i]))))
			}
		}
	}

	it := vm.filter(int(inum), cs)
	more := it.Next()
	if !more {
		if err := it.Err(); err != nil {
			C.go_free(unsafe.Pointer(*cerr))
			*cerr = C.go_strcpy(err.Error())
			return C.SQLITE_ERROR
		}
		return NoMoreRow
	}

	storeHandle(uintptr(cursr), it)
	return C.SQLITE_OK
}

const NoMoreRow C.int = -1

//export goNext
func goNext(cursr C.uintptr_t, cerr **C.char) C.int {
	it := accessHandle(uintptr(cursr)).(Iter[any])
	more := it.Next()
	if !more {
		if err := it.Err(); err != nil {
			C.go_free(unsafe.Pointer(*cerr))
			*cerr = C.go_strcpy(err.Error())
			return C.SQLITE_ERROR
		}

		return NoMoreRow
	}
	// TODO(rdo) goNext should also update rowID in cursor

	return C.SQLITE_OK
}

//export goBindColumn
func goBindColumn(vtab C.uintptr_t, cursr C.uintptr_t, ctx *C.sqlite3_context, col C.int) C.int {
	vm := cgo.Handle(vtab).Value().(rVM)
	it := accessHandle(uintptr(cursr)).(Iter[any])

	j := int(col)
	for i := 0; i <= j; i++ {
		// note that j is always less than the number of fields, so this actually terminates
		if (1<<i)&vm.ignored != 0 {
			j++
			continue
		}
	}

	v := reflect.ValueOf(it.Value()).Field(j)
	switch v := v.Interface().(type) {
	case string:
		C.sqlite3_result_text(ctx, C.go_strcpy(v), -1, (*[0]byte)(C.sqlite3_free))
	case int:
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(v))
	case bool:
		if v {
			C.sqlite3_result_int64(ctx, 1)
		} else {
			C.sqlite3_result_int64(ctx, 0)
		}
	case NullString:
		if len(v) > 0 {
			C.sqlite3_result_text(ctx, C.go_strcpy(string(v)), -1, (*[0]byte)(C.sqlite3_free))
		} else {
			C.sqlite3_result_null(ctx)
		}
	case json.RawMessage:
		var p *byte
		if len(v) > 0 {
			p = &v[0]
		}
		C.sqlite3_result_blob(ctx, unsafe.Pointer(p), C.int(len(v)), C.SQLITE_TRANSIENT)
	case []byte:
		var p *byte
		if len(v) > 0 {
			p = &v[0]
		}
		C.sqlite3_result_blob(ctx, unsafe.Pointer(p), C.int(len(v)), C.SQLITE_TRANSIENT)
	}
	if tt := v.Type(); tt.Kind() == reflect.Pointer && tt.Elem().Kind() == reflect.Struct {
		C.sqlite_ResultGoPointer(ctx, C.uintptr_t(allocHandle(v.Interface())), namefor(tt.Elem()))
	}
	return C.SQLITE_OK
}

//export goRowID
func goRowID(vtab C.uintptr_t, cursr C.uintptr_t) C.sqlite3_int64 {
	vm := cgo.Handle(vtab).Value().(rVM)
	it := accessHandle(uintptr(cursr)).(Iter[any])
	return C.sqlite3_int64(vm.rowid(it.Value()))
}

//export goFindFunction
func goFindFunction(vtab C.uintptr_t, cname *C.char, dst *C.uintptr_t) int {
	vm := cgo.Handle(vtab).Value().(rVM)
	name := C.GoString(cname)
	for i, n := range vm.overloads {
		if name == n.name {
			*dst = C.uintptr_t(n.cm)
			if n.ixer {
				return C.SQLITE_INDEX_CONSTRAINT_FUNCTION + i
			} else {
				return 1
			}
		}
	}

	return 0
}

//export goUpdate
func goUpdate(vtab C.uintptr_t, db *C.sqlite3, argc C.int, argv **C.sqlite3_value,
	rowid *C.sqlite3_int64, errmsg **C.char) int {
	vm := cgo.Handle(vtab).Value().(rVM)
	if vm.update == nil {
		return C.SQLITE_READONLY
	}
	// sqlite uses mutex to protect the sqlite3_step making the call to this function
	// accessing the same DB will case a deadlock, except if we use the same thread – SQLite uses PTHREAD_MUTEX_RECURSIVE
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	args := (*[1 << 10]*C.sqlite3_value)(unsafe.Pointer(argv))
	ctx := context.WithValue(context.Background(), ckey{}, &Conn{db: db})

	if argc == 1 {
		// delete
		err := vm.update(ctx, int64(C.sqlite3_value_int64(args[0])), reflect.Zero(vm.rtype).Interface())
		if err != nil {
			*errmsg = C.go_strcpy(err.Error())
			return C.SQLITE_ERROR
		}
		return C.SQLITE_OK
	}

	index := int64(-1)
	if C.sqlite3_value_type(args[0]) == C.SQLITE_INTEGER {
		index = int64(C.sqlite3_value_int64(args[0]))
	}

	v := reflect.New(vm.rtype).Elem()
	for i := 2; i < int(argc); i++ {
		if vm.ignored.Test(i - 2) {
			continue
		}

		switch C.sqlite3_value_type(args[i]) {
		case C.SQLITE_NULL:
			continue
		case C.SQLITE_TEXT:
			v.Field(i - 2).SetString(C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_value_text(args[i])))))
		case C.SQLITE_INTEGER:
			switch v.Field(i - 2).Kind() {
			case reflect.Int:
				v.Field(i - 2).SetInt(int64(C.sqlite3_value_int64(args[i])))
			case reflect.Bool:
				v.Field(i - 2).SetBool(int64(C.sqlite3_value_int64(args[i])) == 1)
			}
		case C.SQLITE_BLOB:
			l := C.sqlite3_value_bytes(args[i])
			v.Field(i - 2).SetBytes(([]byte)(unsafe.Slice((*byte)(C.sqlite3_value_blob(args[i])), l)))
		}
	}
	if err := vm.update(ctx, index, v.Interface()); err != nil {
		*errmsg = C.go_strcpy(err.Error())
		return C.SQLITE_ERROR
	}

	*rowid = C.sqlite_int64(vm.rowid(v.Interface()))
	return C.SQLITE_OK
}
