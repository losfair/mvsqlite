#define _GNU_SOURCE

#include "shim.h"

static void __attribute__((constructor)) preload_init(void) {
    mvsqlite_global_init();
}
