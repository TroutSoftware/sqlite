package sqlite

/*
#include <amalgamation/sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

// not safe to coerce uintptr to unsafe.Pointer, so C it is
static inline void * unsafe_ptr(uintptr_t hdl) { return (void*)hdl; }

extern void funcTrampoline(sqlite3_context *ctx, int argc, sqlite3_value *argv[argc]);
extern char * go_strcpy(_GoString_ st);
extern void go_free(void*);
extern void sqlite_ResultGoPointer(sqlite3_context* ctx, uintptr_t ptr, const char* name);
*/
import "C"
import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/cgo"
	"unsafe"
)

type callmachine struct {
	ops []rop
	f   reflect.Value

	pn []*C.char
	tt []reflect.Type
}

type rop int

const (
	ropInvalid rop = iota
	ropBindText
	ropBindBlob
	ropBindInt
	ropBindPtr

	ropReturnText // marker value
	ropReturnBlob
	ropReturnInt
	ropReturnBool
	ropReturnPtr
	ropReturnErr
	ropReturnNullString

	ropVariadic rop = 0x100
)

func typefor[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

var (
	boolType   = typefor[bool]()
	intType    = typefor[int]()
	txtType    = typefor[string]()
	blbType    = typefor[[]byte]()
	errType    = typefor[error]()
	ptrType    = typefor[PointerValue]()
	nullstType = typefor[NullString]()
)

func reflectCallMachine(t any) callmachine {
	tt := reflect.TypeOf(t)
	cm := callmachine{f: reflect.ValueOf(t)}
	for i := 0; i < tt.NumIn(); i++ {
		ta := tt.In(i)

		var varmark rop
		if tt.IsVariadic() && i == tt.NumIn()-1 {
			varmark = ropVariadic
			ta = ta.Elem()
		}

		// three base types
		switch ta {
		case intType:
			cm.ops = append(cm.ops, ropBindInt|varmark)
		case txtType, nullstType:
			cm.ops = append(cm.ops, ropBindText|varmark)
		case blbType:
			cm.ops = append(cm.ops, ropBindBlob|varmark)
		}

		// arbitrary values as pointers
		if ta.Kind() == reflect.Pointer {
			cm.ops = append(cm.ops, ropBindPtr|varmark)
			cm.pn = append(cm.pn, namefor(tt.In(i).Elem()))
			cm.tt = append(cm.tt, reflect.PointerTo(tt.In(i).Elem()))
		}
	}

	switch tt.NumOut() {
	case 0, 1:
		// all good
	case 2:
		// second must be error type
		if tt.Out(1) != errType {
			panic("second type must be error")
		}
	default:
		panic("invalid number of output arguments")
	}
	for i := 0; i < tt.NumOut(); i++ {
		switch tt.Out(i) {
		case intType:
			cm.ops = append(cm.ops, ropReturnInt)
		case txtType:
			cm.ops = append(cm.ops, ropReturnText)
		case blbType:
			cm.ops = append(cm.ops, ropReturnBlob)
		case ptrType:
			cm.ops = append(cm.ops, ropReturnPtr)
		case boolType:
			cm.ops = append(cm.ops, ropReturnBool)
		case nullstType:
			cm.ops = append(cm.ops, ropReturnNullString)
		case errType:
			cm.ops = append(cm.ops, ropReturnErr)
		default:
			panic("not a known output type")
		}
	}

	return cm
}

// RegisterFunc binds a Go function as an [application-defined SQL function].
// Functions can only be exposed if:
//
//  1. Their inputs arguments are integer, text, bytes or a pointer to an arbitrary type
//  2. Their name is not a registered SQL keyword
//  3. They return either
//     (1) no argument,
//     (2) a single argument of type integer, text, bytes, error or [PointerValue] or
//     (3) 2 arguments of which
//     the first argument must be of type integer, text, bytes or [PointerValue]
//     the second argument must be of type erorr.
//
// [application-defined SQL function]: https://www.sqlite.org/appfunc.html
func RegisterFunc(name string, t any) func(SQLITE3) {
	return func(db SQLITE3) {
		tt := reflect.TypeOf(t)
		if tt.Kind() != reflect.Func {
			panic("RegisterFunc only accepts functions")
		}

		p := &([]byte(name)[0])
		if C.sqlite3_keyword_check((*C.char)(unsafe.Pointer(p)), C.int(len(name))) != 0 {
			panic("do not use SQL identifiers as function names")
		}

		nargs := tt.NumIn()
		if tt.IsVariadic() {
			nargs = -1
		}

		cn := C.go_strcpy(name)
		rc := C.sqlite3_create_function(
			(*C.sqlite3)(db),
			cn,
			C.int(nargs),
			C.SQLITE_UTF8|C.SQLITE_DETERMINISTIC|C.SQLITE_DIRECTONLY,
			C.unsafe_ptr(C.uintptr_t(cgo.NewHandle(reflectCallMachine(t)))),
			(*[0]byte)(C.funcTrampoline),
			nil,
			nil,
		)
		if rc != C.SQLITE_OK {
			panic(fmt.Sprintf("invalid result: %d", rc))
		}
		C.go_free(unsafe.Pointer(cn))
	}
}

//export callGoNativeFunc
func callGoNativeFunc(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	cm := cgo.Handle(C.sqlite3_user_data(ctx)).Value().(callmachine)
	goargv := unsafe.Slice(argv, argc)

	args := make([]reflect.Value, argc)
	j, k := 0, 0
	for i := range goargv {
		op := cm.ops[j]
		if op > ropVariadic {
			op ^= ropVariadic
		}

		switch op {
		case ropBindText:
			// we know, by compilation, this is valid utf8
			args[i] = reflect.ValueOf(C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_value_text(goargv[i])))))
		case ropBindBlob:
			// byte cast is required to defeat pointer conversion
			l := C.sqlite3_value_bytes(goargv[i])
			args[i] = reflect.ValueOf(([]byte)(unsafe.Slice((*byte)(C.sqlite3_value_blob(goargv[i])), l)))
		case ropBindInt:
			args[i] = reflect.ValueOf(int(C.sqlite3_value_int(goargv[i])))
		case ropBindPtr:
			if v := uintptr(C.sqlite3_value_pointer(goargv[i], cm.pn[k])); v != 0 {
				args[i] = reflect.ValueOf(accessHandle(v))
			} else {
				args[i] = reflect.Zero(cm.tt[k])
			}
			k++
		default:
			panic("invalid op")
		}

		if cm.ops[j] < ropVariadic {
			j++
		}
	}

	tm := cm.f.Call(args)
	switch len(tm) {
	case 0:
		return
	case 2:
		// this is an error type, checked during function registration
		err, ok := tm[1].Interface().(error)
		if ok && err != nil {
			if errors.Is(err, ErrNull) {
				C.sqlite3_result_null(ctx)
				return
			}

			spitError(ctx, err)
			return
		}
	}

	// move ptr past varargs
	if cm.ops[j] > ropVariadic {
		j++
	}

	switch cm.ops[j] {
	case ropReturnBlob:
		v := tm[0].Interface().([]byte)
		var p *byte
		if len(v) > 0 {
			p = &v[0]
		}
		C.sqlite3_result_blob(ctx, unsafe.Pointer(p), C.int(len(v)), C.SQLITE_TRANSIENT)
		runtime.KeepAlive(v)
	case ropReturnText:
		v := tm[0].Interface().(string)
		dst := C.go_strcpy(v)
		runtime.KeepAlive(v)
		C.sqlite3_result_text(ctx, dst, -1, (*[0]byte)(C.sqlite3_free))
	case ropReturnInt:
		v := tm[0].Interface().(int)
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(v))
	case ropReturnErr:
		err, ok := tm[0].Interface().(error)
		if ok && err != nil {
			spitError(ctx, err)
		}
	case ropReturnBool:
		v := tm[0].Interface().(bool)
		if v {
			C.sqlite3_result_int64(ctx, 1)
		} else {
			C.sqlite3_result_int64(ctx, 0)
		}
	case ropReturnPtr:
		ptr := tm[0].Interface().(PointerValue)
		C.sqlite_ResultGoPointer(ctx, C.uintptr_t(allocHandle(ptr.v)), namefor(reflect.TypeOf(ptr.v).Elem()))
	case ropReturnNullString:
		v := tm[0].Interface().(NullString)
		if len(v) > 0 {
			C.sqlite3_result_text(ctx, C.go_strcpy(string(v)), -1, (*[0]byte)(C.sqlite3_free))
		} else {
			C.sqlite3_result_null(ctx)
		}
	}
}

// ErrNull is a sentinel value that can be returned by functions that want to return (or otherwise handle) null values
var ErrNull = errors.New("null SQL value")

func spitError(ctx *C.sqlite3_context, err error) {
	msg := ([]byte)(err.Error())
	var p *byte
	if len(msg) > 0 {
		p = &msg[0]
	}
	C.sqlite3_result_error(ctx, (*C.char)(unsafe.Pointer(p)), C.int(len(msg)))
	runtime.KeepAlive(msg)
	// SQLite makes a copy, per doc
}
