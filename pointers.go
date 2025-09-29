package sqlite

import (
	"reflect"
	"runtime/cgo"
	"sync"
)

// #include <stdint.h>
// extern char * go_strcpy(_GoString_ st);
import "C"

// Marker type for values that should be passed as pointer.
// Most users will prefer [AsPointer].
type PointerValue cgo.Handle

// static (compile-time) name for a pointer
//
// the C documentation is pretty clear about the need to have static strings as
// identifiers for the type of the pointer passed in functions.
// Go does offer better runtime introspection, so we use this to remove the noise.
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
// This interface is only useful for reading the value in virtual functions.
// The “pointer type” parameter will be derived from the underlying data type name.
//
// [interface]: https://sqlite.org/bindptr.html
func (ctn *Conn) AsPointer(v any) PointerValue {
	if reflect.TypeOf(v).Kind() != reflect.Pointer && reflect.TypeOf(v).Elem().Kind() != reflect.Struct {
		panic("AsPointer must be given a pointer to a struct")
	}
	ctn.pointers.Pin(v)
	return PointerValue(cgo.NewHandle(v))
}

//export cleanupGoHandle
func cleanupGoHandle(hdl C.uintptr_t) { 
	cgo.Handle(hdl).Delete() 
}
