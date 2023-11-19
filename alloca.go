// note: this file is included in all following go files
// it must be the first in the repository so translations unit work that way :shrug:

package sqlite

/*
#include <string.h>
#include <amalgamation/sqlite3.h>

static inline char * go_strcpy(_GoString_ st) {
	char* buf = sqlite3_malloc64(_GoStringLen(st) + 1);
	memcpy(buf, _GoStringPtr(st), _GoStringLen(st));
	buf[_GoStringLen(st)] = 0;
	return buf;
}

static inline void go_free(void * mem) {
	sqlite3_free(mem);
}
*/
import "C"
import "unsafe"

func init() {
	C.go_free(unsafe.Pointer(C.go_strcpy("")))
}
