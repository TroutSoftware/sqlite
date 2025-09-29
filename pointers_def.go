package sqlite

/*
#include <amalgamation/sqlite3.h>
#include <stdint.h>

#include <stdio.h>

extern void cleanupGoHandle(uintptr_t);

void wrapGoCleanup(void *handle) { cleanupGoHandle((uintptr_t) handle); }

void sqlite_ResultGoPointer(sqlite3_context* ctx, uintptr_t ptr, const char* name) {
	sqlite3_result_pointer(ctx, (void*)ptr, name, wrapGoCleanup);
}

int sqlite_BindGoPointer(sqlite3_stmt* stmt, int pos, uintptr_t ptr, const char* name) {
	return sqlite3_bind_pointer(stmt, pos, (void*)ptr, name, wrapGoCleanup);
}
*/
import "C"
