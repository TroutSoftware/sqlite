//go:build !sharedlib

package sqlite

// #include "./amalgamation/sqlite3.c"
// #cgo CFLAGS: -DSQLITE_ENABLE_API_ARMOR -DSQLITE_ENABLE_EXPLAIN_COMMENTS -DSQLITE_ENABLE_UNKNOWN_SQL_FUNCTION  -DSQLITE_OMIT_LOAD_EXTENSION  -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
// #cgo CFLAGS: -DSQLITE_THREADSAFE -DSQLITE_LIKE_DOESNT_MATCH_BLOBS -DSQLITE_USE_ALLOCA -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_UTF16 -DSQLITE_OMIT_SHARED_CACHE -DSQLITE_ENABLE_NORMALIZE
// #cgo CFLAGS: -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_FTS5 -DSQLITE_SOUNDEX -DSQLITE_ENABLE_BYTECODE_VTAB -DSQLITE_ENABLE_STMTVTAB
// #cgo CFLAGS: -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 -DSQLITE_DEFAULT_WORKER_THREADS=4 -DSQLITE_MAX_WORKER_THREADS=50  -DSQLITE_ENABLE_SETLK_TIMEOUT
// #cgo LDFLAGS: -lm
import "C"
