#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <amalgamation/sqlite3.h>

extern int goGetTableDefinition(uintptr_t, char **);
extern int goSetBestIndex(uintptr_t, sqlite3_index_info *);
extern int goFilter(uintptr_t, uintptr_t, int, const char *, int,
                    sqlite3_value **, char **);
extern uintptr_t goAllocCursor(uintptr_t);
extern int goNext(uintptr_t, char **);
extern int goBindColumn(uintptr_t, uintptr_t, sqlite3_context *, int);
extern sqlite3_int64 goRowID(uintptr_t, uintptr_t);
extern void sqlite_FreeHandle(uintptr_t);
extern int goFindFunction(uintptr_t, const char *, uintptr_t *);
extern void funcTrampoline(sqlite3_context *, int argc,
                           sqlite3_value *argv[argc]);
extern int goUpdate(uintptr_t, sqlite3 *, int argc,
                    sqlite3_value *argv[argc], sqlite_int64 *rowid,
                    char **errmsg);

typedef struct generic_vtab generic_vtab;
struct generic_vtab {
  sqlite3_vtab vtab;
  sqlite3_int64 rowid;
  sqlite3 *db;
  uintptr_t impl;
};

/*
vtConnect opens a new connection to the underlying virtual table.

The Go side is responsible to create the table definition through the values
passed in the [RegisterTable] call.
*/
static int vtConnect(sqlite3 *db, void *aux, int argc, const char *const *argv,
                     sqlite3_vtab **vtab, char **err) {
  char *st;
  int rc = goGetTableDefinition((uintptr_t)aux, &st);
  if (rc != SQLITE_OK)
    return rc;

  rc = sqlite3_declare_vtab(db, st);
  sqlite3_free(st);
  if (rc != SQLITE_OK)
    return rc;

  generic_vtab *vt = sqlite3_malloc(sizeof *vt);
  if (vt == 0)
    return SQLITE_NOMEM;

  memset(vt, 0, sizeof *vt);
  vt->impl = (uintptr_t)aux;
  vt->db = db;

  *vtab = (sqlite3_vtab *)vt;
  return SQLITE_OK;
}

// vtDisconnect destructs the virtual table C-side
static int vtDisconnect(sqlite3_vtab *vtab) {
  sqlite3_free((generic_vtab *)vtab);
  return SQLITE_OK;
}

// generic_cursor provides convenience around counting things
typedef struct generic_cursor generic_cursor;
struct generic_cursor {
  sqlite3_vtab_cursor cursor;
  uintptr_t go_impl;
  bool eof;
};

// vtOpen allocates a new cursor
static int vtOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **vt_cur) {
  generic_cursor *cur = sqlite3_malloc(sizeof *cur);
  if (cur == 0)
    return SQLITE_NOMEM;

  memset(cur, 0, sizeof *cur);
  cur->go_impl = goAllocCursor((uintptr_t)cur);
  *vt_cur = (sqlite3_vtab_cursor *)cur;
  return SQLITE_OK;
}

static int vtRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *rowid) {
  generic_cursor *gc = (generic_cursor *)cur;
  generic_vtab *tab = (generic_vtab *)cur->pVtab;
  sqlite3_int64 rid = goRowID(tab->impl, gc->go_impl);
  if (rid != -1) {
    *rowid = rid;
  } else {
    *rowid = ((generic_vtab *)cur->pVtab)->rowid;
  }

  return SQLITE_OK;
}

static int vtEOF(sqlite3_vtab_cursor *vcur) {
  generic_cursor *gc = (generic_cursor *)vcur;
  return gc->eof;
}

// vtClose frees the cursor
static int vtClose(sqlite3_vtab_cursor *vcur) {
  generic_cursor *gc = (generic_cursor *)vcur;
  sqlite_FreeHandle(gc->go_impl);
  sqlite3_free(gc);
  return SQLITE_OK;
}

static int vtNext(sqlite3_vtab_cursor *cur) {
  enum {
    NoMoreRows = -1,
  };

  generic_cursor *gc = (generic_cursor *)cur;
  ((generic_vtab *)cur->pVtab)->rowid++;
  int rc = goNext(gc->go_impl, &cur->pVtab->zErrMsg);
  if (rc == NoMoreRows) {
    gc->eof = true;
    return SQLITE_OK;
  }

  return rc;
}

static int vtFilter(sqlite3_vtab_cursor *cur, int idxNum, const char *idxStr,
                    int argc, sqlite3_value *argv[argc]) {
  generic_cursor *gc = (generic_cursor *)cur;
  generic_vtab *vtab = (generic_vtab *)cur->pVtab;
  gc->eof = false;
  int rc = goFilter(vtab->impl, gc->go_impl, idxNum, idxStr, argc, argv,
                    &(cur->pVtab->zErrMsg));
  if (rc == -1) {
    gc->eof = true;
    return SQLITE_OK;
  }
  ((generic_vtab *)cur->pVtab)->rowid++;
  return rc;
}

static int vtColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int N) {
  generic_cursor *gc = (generic_cursor *)cur;
  generic_vtab *tab = (generic_vtab *)cur->pVtab;
  return goBindColumn(tab->impl, gc->go_impl, ctx, N);
}

static int vtBestIndex(sqlite3_vtab *vtab, sqlite3_index_info *idx) {
  return goSetBestIndex(((generic_vtab *)vtab)->impl, idx);
}

static int vtFindFunction(sqlite3_vtab *vtab, int argc, const char *name,
                          void (**fn)(sqlite3_context *, int, sqlite3_value **),
                          void **arg) {
  int index =
      goFindFunction(((generic_vtab *)vtab)->impl, name, (uintptr_t *)arg);
  if (index == 0)
    return 0;

  *fn = funcTrampoline;
  return index;
}

static int vtUpdate(sqlite3_vtab *vtab, int argc, sqlite3_value **argv,
                    sqlite_int64 *rowid) {
  generic_vtab *tab = (generic_vtab *)vtab;
  return goUpdate(tab->impl, tab->db, argc, argv, rowid, &vtab->zErrMsg);
}

static sqlite3_module generic_vtable = {
    .xConnect = vtConnect,
    .xBestIndex = vtBestIndex,
    .xDisconnect = vtDisconnect,
    .xOpen = vtOpen,
    .xClose = vtClose,
    .xFilter = vtFilter,
    .xRowid = vtRowid,
    .xNext = vtNext,
    .xEof = vtEOF,
    .xColumn = vtColumn,
    .xFindFunction = vtFindFunction,
    .xUpdate = vtUpdate,
};

int register_new_vtable(sqlite3 *db, const char *name, uintptr_t go_handle,
                        char **pzErrMsg) {
  int rc = SQLITE_OK;
  rc = sqlite3_create_module(db, name, &generic_vtable, (void *)go_handle);
  return rc;
}
