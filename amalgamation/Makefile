# Useful for local development: compile binary once, and link it with CGO

OPTS := -DSQLITE_ENABLE_API_ARMOR -DSQLITE_ENABLE_EXPLAIN_COMMENTS -DSQLITE_ENABLE_UNKNOWN_SQL_FUNCTION -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
OPTS += -DSQLITE_THREADSAFE -DSQLITE_LIKE_DOESNT_MATCH_BLOBS -DSQLITE_USE_ALLOCA -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_UTF16 -DSQLITE_OMIT_SHARED_CACHE
OPTS += -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_FTS5 -DSQLITE_SOUNDEX -DSQLITE_ENABLE_BYTECODE_VTAB -DSQLITE_ENABLE_STMTVTAB -DSQLITE_ENABLE_NORMALIZE
OPTS += -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 -DSQLITE_DEFAULT_WORKER_THREADS=4 -DSQLITE_MAX_WORKER_THREADS=50 -DSQLITE_ENABLE_SETLK_TIMEOUT

LDLIBS := -lpthread -ldl -lm

ifeq ("$(ASAN)", "1")
	SANITIZER := -fsanitize=address
	LDLIBS +=  -lasan
endif

install: shell
	mkdir -p $(HOME)/.local/bin
	mv shell $(HOME)/.local/bin/sqlite3
	cp libsqlite3.so $(HOME)/.local/bin/libsqlite3.so

libsqlite3.so: sqlite3.c
	gcc -o $@ $(OPTS) -shared -fPIC -ggdb -O1 -fno-omit-frame-pointer $(SANITIZER) $< $(LDLIBS)

shell: shell.c | libsqlite3.so
	cc -o shell -DHAVE_READLINE -ggdb -O2 $<  -Wl,-rpath,'$$ORIGIN' -L$(CURDIR) $(LDLIBS) -lsqlite3 -lreadline

.PHONY: clean install
clean:
	rm shell libsqlite3.so