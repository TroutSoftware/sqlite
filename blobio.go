package sqlite

/*
#include <amalgamation/sqlite3.h>

extern char * go_strcpy(_GoString_ st);
extern void go_free(void*);
*/
import "C"
import "unsafe"

import (
	"context"
	"fmt"
)

// ReadWriterAt is an incremental I/O buffer.
// It is mostly useful to work directly with binary formats in SQLite – so akin to mmap.
type DirectBLOB struct {
	blob *C.sqlite3_blob
	cn   *Conn

	RowNumber int

	database, table, column string
}

// CreateBLOB creates a new incremental I/O buffer.
// The database, table, and column are only quoted, and therefore should not be from untrusted inputs.
func (cn *Conn) CreateBLOB(database, table, column string, size int) (*DirectBLOB, error) {
	var rownum int
	err := cn.Exec(context.Background(), fmt.Sprintf(`insert into "%s"."%s" ("%s") values (zeroblob(?)) returning rowid`, database, table, column), size).ScanOne(&rownum)
	if err != nil {
		return nil, err
	}

	return cn.OpenBLOB(database, table, column, rownum)
}

func (cn *Conn) OpenBLOB(database, table, column string, rownum int) (*DirectBLOB, error) {
	const readAccess = 0
	const writeAccess = 1

	cdb := C.go_strcpy(database)
	defer C.go_free(unsafe.Pointer(cdb))
	ctn := C.go_strcpy(table)
	defer C.go_free(unsafe.Pointer(ctn))
	col := C.go_strcpy(column)
	defer C.go_free(unsafe.Pointer(col))

	var blob *C.sqlite3_blob

	cn.mxdb.Lock()
	rc := C.sqlite3_blob_open(cn.db, cdb, ctn, col, C.sqlite3_int64(rownum), writeAccess, &blob)
	if rc != C.SQLITE_OK {
		return nil, cn.error(rc)
	}
	cn.mxdb.Unlock()

	return &DirectBLOB{cn: cn, blob: blob, RowNumber: rownum,
		database: database, table: table, column: column}, nil
}

func (r *DirectBLOB) ReadAt(p []byte, off int64) (n int, err error) {
	r.cn.mxdb.Lock()
	rc := C.sqlite3_blob_read(r.blob, unsafe.Pointer(&p[0]), C.int(len(p)), C.int(off))
	if rc == C.SQLITE_OK {
		r.cn.mxdb.Unlock()
		return len(p), nil
	}

	return 0, r.cn.error(rc)
}

func (r *DirectBLOB) WriteAt(p []byte, off int64) (n int, err error) {
	r.cn.mxdb.Lock()
	rc := C.sqlite3_blob_write(r.blob, unsafe.Pointer(&p[0]), C.int(len(p)), C.int(off))
	if rc == C.SQLITE_OK {
		r.cn.mxdb.Unlock()
		return len(p), nil
	}

	return 0, r.cn.error(rc)
}

func (r *DirectBLOB) Size() int { return int(C.sqlite3_blob_bytes(r.blob)) }

func (r *DirectBLOB) Close() error {
	r.cn.mxdb.Lock()
	rc := C.sqlite3_blob_close(r.blob)
	r.blob = nil // prevent misuse, nullptr is a no-op in SQLite
	return r.cn.error(rc)
}
