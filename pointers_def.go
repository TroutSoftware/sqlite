package sqlite

/*
#include <amalgamation/sqlite3.h>
#include <stdint.h>

extern void sqlite_FreeHandle(uintptr_t);

static inline void wrap_freeHandle(void *p) {
	sqlite_FreeHandle((uintptr_t)p);
}

void sqlite_ResultGoPointer(sqlite3_context* ctx, uintptr_t ptr, const char* name) {
	sqlite3_result_pointer(ctx, (void*)ptr, name, wrap_freeHandle);
}

int sqlite_BindGoPointer(sqlite3_stmt* stmt, int pos, uintptr_t ptr, const char* name) {
	return sqlite3_bind_pointer(stmt, pos, (void*)ptr, name, wrap_freeHandle);
}
*/
import "C"
