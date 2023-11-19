package sqlite

/*
#include <stdlib.h>
#include <amalgamation/sqlite3.h>

extern char * go_strcpy(_GoString_ st);
extern void go_free(void*);
*/
import "C"
import "unsafe"

// IsComplete returns true iff the statement is complete
func IsComplete(st string) bool {
	cst := C.go_strcpy(st)
	full := C.sqlite3_complete(cst)
	C.go_free(unsafe.Pointer(cst))

	return full == 1
}
