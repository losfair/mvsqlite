#define _GNU_SOURCE

#include <stdlib.h>
#include <dlfcn.h>
#include <stdio.h>
#include <pthread.h>
#include <sqlite3.h>
#include <assert.h>

extern void init_mvsqlite(void);

typedef int (*sqlite3_initialize_fn)(void);
typedef int (*sqlite3_open_v2_fn)(
    const char *filename,   /* Database filename (UTF-8) */
    sqlite3 **ppDb,            /* OUT: SQLite db handle */
    int flags,              /* Flags */
    const char *zVfs        /* Name of VFS module to use */
);

static sqlite3_open_v2_fn real_sqlite3_open_v2 = NULL;

static void __attribute__((constructor)) preload_init(void) {
    real_sqlite3_open_v2 = dlsym(RTLD_NEXT, "sqlite3_open_v2");
    if (real_sqlite3_open_v2 == NULL) {
        fprintf(stderr, "Failed to find real sqlite3_open_v2\n");
        exit(1);
    }
}

static pthread_once_t vfs_init = PTHREAD_ONCE_INIT;

int sqlite3_open_v2(
    const char *filename,   /* Database filename (UTF-8) */
    sqlite3 **ppDb,            /* OUT: SQLite db handle */
    int flags,              /* Flags */
    const char *zVfs        /* Name of VFS module to use */
) {
    int ret;

    pthread_once(&vfs_init, init_mvsqlite);
    ret = real_sqlite3_open_v2(filename, ppDb, flags, zVfs);
    if(ret == SQLITE_OK) {
        ret = sqlite3_exec(*ppDb, "PRAGMA journal_mode = memory", NULL, NULL, NULL);
        assert(ret == SQLITE_OK);

        ret = sqlite3_exec(*ppDb, "PRAGMA cache_size = -50000", NULL, NULL, NULL);
        assert(ret == SQLITE_OK);
    }
    return ret;
}

int sqlite3_open(
    const char *filename,   /* Database filename (UTF-8) */
    sqlite3 **ppDb          /* OUT: SQLite db handle */
) {
    return sqlite3_open_v2(filename, ppDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
}

int sqlite3_open16(
    const void *filename,   /* Database filename (UTF-16) */
    sqlite3 **ppDb          /* OUT: SQLite db handle */
) {
    abort();
}
