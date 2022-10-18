#define _GNU_SOURCE

#include <stdlib.h>
#include <dlfcn.h>
#include <stdio.h>
#include <pthread.h>
#include <sqlite3.h>
#include <assert.h>

extern void init_mvsqlite(void);
extern void init_mvsqlite_connection(sqlite3 *db);
extern void mvsqlite_autocommit_backoff(sqlite3 *db);

typedef int (*sqlite3_initialize_fn)(void);
typedef int (*sqlite3_open_v2_fn)(
    const char *filename,   /* Database filename (UTF-8) */
    sqlite3 **ppDb,            /* OUT: SQLite db handle */
    int flags,              /* Flags */
    const char *zVfs        /* Name of VFS module to use */
);
typedef int (*sqlite3_step_fn)(sqlite3_stmt *pStmt);

#ifdef MV_STATIC_PATCH
int real_sqlite3_open_v2(
    const char *filename,   /* Database filename (UTF-8) */
    sqlite3 **ppDb,            /* OUT: SQLite db handle */
    int flags,              /* Flags */
    const char *zVfs        /* Name of VFS module to use */
);
int real_sqlite3_step(sqlite3_stmt *pStmt);
#else
static sqlite3_open_v2_fn real_sqlite3_open_v2 = NULL;
static sqlite3_step_fn real_sqlite3_step = NULL;
#endif

static int mvsqlite_enabled = 0;

void mvsqlite_global_init(void) {
    mvsqlite_enabled = 1;
}

static void bootstrap(void) {
#ifndef MV_STATIC_PATCH
    real_sqlite3_open_v2 = dlsym(RTLD_NEXT, "sqlite3_open_v2");
    if (real_sqlite3_open_v2 == NULL) {
        fprintf(stderr, "Failed to find real sqlite3_open_v2\n");
        exit(1);
    }

    real_sqlite3_step = dlsym(RTLD_NEXT, "sqlite3_step");
    if (real_sqlite3_step == NULL) {
        fprintf(stderr, "Failed to find real sqlite3_step\n");
        exit(1);
    }
#endif

    if(mvsqlite_enabled) {
        init_mvsqlite();
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

    pthread_once(&vfs_init, bootstrap);
    ret = real_sqlite3_open_v2(filename, ppDb, flags, zVfs);
    if(ret == SQLITE_OK && mvsqlite_enabled) {
        init_mvsqlite_connection(*ppDb);
    }
    return ret;
}

int sqlite3_open(
    const char *filename,   /* Database filename (UTF-8) */
    sqlite3 **ppDb          /* OUT: SQLite db handle */
) {
    return sqlite3_open_v2(filename, ppDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
}

static __thread int in_sqlite3_step = 0;

int sqlite3_step(sqlite3_stmt *pStmt) {
    int ret;
    int autocommit;
    sqlite3 *db;

    if(in_sqlite3_step) {
        return real_sqlite3_step(pStmt);
    }
    
    in_sqlite3_step = 1;
    db = sqlite3_db_handle(pStmt);

    while (1) {
        autocommit = sqlite3_get_autocommit(db);
        ret = real_sqlite3_step(pStmt);
        if(ret == SQLITE_BUSY && mvsqlite_enabled && autocommit) {
            mvsqlite_autocommit_backoff(db);
        } else {
            in_sqlite3_step = 0;
            return ret;
        }
    }
}
