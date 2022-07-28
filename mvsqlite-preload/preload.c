#define _GNU_SOURCE

#include <stdlib.h>
#include <dlfcn.h>
#include <stdio.h>
#include <pthread.h>

extern void init_mvsqlite(void);

typedef int (*sqlite3_initialize_fn)(void);
typedef int (*sqlite3_open_v2_fn)(
    const char *filename,   /* Database filename (UTF-8) */
    void **ppDb,            /* OUT: SQLite db handle */
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
    void **ppDb,            /* OUT: SQLite db handle */
    int flags,              /* Flags */
    const char *zVfs        /* Name of VFS module to use */
) {
    pthread_once(&vfs_init, init_mvsqlite);
    return real_sqlite3_open_v2(filename, ppDb, flags, zVfs);
}
