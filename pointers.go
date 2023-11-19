package sqlite

import (
	"os"
	"reflect"
	"runtime/pprof"
	"sync"
)

// #include <stdint.h>
// extern char * go_strcpy(_GoString_ st);
import "C"

var openHandles []any
var mxHandles sync.Mutex

var _handles_profiles = pprof.NewProfile("t.sftw/sqlite/openhandles")

var recordProfiles = os.Getenv("SHDEBUG") == "ptrprof"

type reserve struct{} // for when you need to reserve a spot in the parking lot

func allocHandle(v any) uintptr {
	mxHandles.Lock()
	defer mxHandles.Unlock()

	for i := range openHandles {
		if openHandles[i] == nil {
			openHandles[i] = v
			if recordProfiles {
				_handles_profiles.Add(i, 1)
			}
			return uintptr(i) + 1
		}
	}

	openHandles = append(openHandles, v)
	// handle used as pointer, must be one-based
	if recordProfiles {
		_handles_profiles.Add(len(openHandles)-1, 1)
	}
	return uintptr(len(openHandles))
}

func accessHandle(i uintptr) any {
	mxHandles.Lock()
	defer mxHandles.Unlock()

	return openHandles[int(i)-1]
}

func storeHandle(i uintptr, v any) {
	mxHandles.Lock()
	defer mxHandles.Unlock()

	openHandles[int(i)-1] = v
}

//export sqlite_FreeHandle
func sqlite_FreeHandle(i C.uintptr_t) {
	mxHandles.Lock()
	defer mxHandles.Unlock()

	if recordProfiles {
		_handles_profiles.Remove(int(i) - 1)
	}
	openHandles[int(i)-1] = nil
}

// Marker type for values that should be passed as pointer.
// Most users will prefer [AsPointer].
type PointerValue struct{ v any }

func namefor(tt reflect.Type) *C.char {
	gn := tt.PkgPath() + "." + tt.Name()
	mxTypeNames.Lock()
	nn, ok := typenames[gn]
	if !ok {
		nn = C.go_strcpy(gn)
		typenames[gn] = nn
	}
	mxTypeNames.Unlock()
	return nn
}

var typenames = make(map[string]*C.char)
var mxTypeNames sync.Mutex

// AsPointer is used to pass the value using the SQLite pointer passing [interface].
// This interface is only useful for reading the value in virtual tables or functions.
// The “pointer type” parameter will be derived from the underlying data type name.
//
// [interface]: https://sqlite.org/bindptr.html
func AsPointer(v any) PointerValue {
	if reflect.TypeOf(v).Kind() != reflect.Pointer && reflect.TypeOf(v).Elem().Kind() != reflect.Struct {
		panic("AsPointer must be given a pointer to a struct")
	}
	return PointerValue{v: v}
}
