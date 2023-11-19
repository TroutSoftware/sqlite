package sqlite

/*
#include <amalgamation/sqlite3.h>
#include <string.h>

extern void callGoNativeFunc(sqlite3_context *ctx, int argc, sqlite3_value *argv[argc]);
void funcTrampoline(sqlite3_context *ctx, int argc, sqlite3_value *argv[argc])
{
 callGoNativeFunc(ctx, argc, argv);
}
*/
import "C"
