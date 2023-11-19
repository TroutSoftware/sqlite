package sqlite

// #include <amalgamation/sqlite3.h>
// #include <stdint.h>
//
// extern char * go_strcpy(_GoString_ st);
// extern void go_free(void*);
// extern int sqlite_BindGoPointer(sqlite3_stmt* stmt, int pos, uintptr_t ptr, const char* name);
import "C"
import "unsafe"

// DBName is the name of the database used in the application (main by default).
//
// It can be changed before a back-up is started to reflect the application specificities.
var DBName = "main"

// BackupDB performs an [online backup] to dest
//
// [online backup] https://www.sqlite.org/backup.html
func BackupDB(pool *Connections, dest string) error {
	ctn := pool.take()
	defer pool.put(ctn)

	var db *C.sqlite3
	cname := C.go_strcpy(dest)
	defer C.go_free(unsafe.Pointer(cname))
	rv := C.sqlite3_open_v2(cname, &db,
		C.SQLITE_OPEN_FULLMUTEX|
			C.SQLITE_OPEN_READWRITE|
			C.SQLITE_OPEN_CREATE|
			C.SQLITE_OPEN_WAL|
			C.SQLITE_OPEN_URI,
		nil)
	if rv != C.SQLITE_OK {
		return errText[rv]
	}
	if db == nil {
		panic("sqlite succeeded without returning a database")
	}

	cmain := C.go_strcpy(DBName)
	defer C.go_free(unsafe.Pointer(cmain))

	back := C.sqlite3_backup_init(db, cmain, ctn.db, cmain)
	if back == nil {
		return errorString{s: C.GoString(C.sqlite3_errmsg(ctn.db))}
	}

	C.sqlite3_backup_step(back, -1)
	if rv := C.sqlite3_backup_finish(back); rv != C.SQLITE_OK {
		return errorString{s: C.GoString(C.sqlite3_errmsg(ctn.db))}
	}

	C.sqlite3_close(db)
	return nil
}
