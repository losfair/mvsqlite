/*
 * prefetch_workloads.c - End-to-end benchmark for mvsqlite prefetcher
 *
 * Targeted SQL workloads that stress different page access patterns.
 * Links against the patched libsqlite3.so (with mvsqlite statically linked).
 *
 * Usage:
 *   ./prefetch_workloads <db_path> setup <workload>   # create tables, populate data
 *   ./prefetch_workloads <db_path> run <workload> [N]  # run workload N times (default 3)
 *   ./prefetch_workloads <db_path> setup all           # setup all workloads
 *   ./prefetch_workloads <db_path> run all [N]         # run all workloads
 *
 * Output (run mode):
 *   RESULT <workload> <median_secs> <min_secs> <max_secs>
 */

#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ---------- PRNG (xorshift32, deterministic) ---------- */

static uint32_t rng_state = 0x12345678;

static void rng_seed(uint32_t s) {
    rng_state = s ? s : 1;
}

static uint32_t rng_next(void) {
    uint32_t x = rng_state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    rng_state = x;
    return x;
}

static uint32_t rng_range(uint32_t max) {
    return rng_next() % max;
}

/* ---------- Timing ---------- */

static double now_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ---------- SQLite helpers ---------- */

static void exec_or_die(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error on [%.60s...]: %s\n", sql, err ? err : "unknown");
        sqlite3_free(err);
        exit(1);
    }
}

static sqlite3_stmt *prepare_or_die(sqlite3 *db, const char *sql) {
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Prepare error on [%.60s...]: %s\n", sql, sqlite3_errmsg(db));
        exit(1);
    }
    return stmt;
}

static void step_done(sqlite3 *db, sqlite3_stmt *stmt) {
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
        fprintf(stderr, "Step error: %s\n", sqlite3_errmsg(db));
        exit(1);
    }
}

/* Drain all rows from a statement, counting them */
static int drain_rows(sqlite3_stmt *stmt) {
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        count++;
    }
    return count;
}

/* Query mv_prefetch_metrics('main') and print the JSON result.
 * Output format: METRICS <workload> <json>
 * The function is a no-op if mv_prefetch_metrics is not available. */
static void print_prefetch_metrics(sqlite3 *db, const char *workload_name) {
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db,
        "SELECT mv_prefetch_metrics('main')", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        /* Function not available (e.g., not running under mvsqlite) */
        return;
    }
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        if (json) {
            printf("METRICS %s %s\n", workload_name, json);
            fflush(stdout);
        }
    }
    sqlite3_finalize(stmt);
}

/* ---------- Padding data for realistic row sizes ---------- */

static const char padding_chars[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

static void fill_padding(char *buf, int len) {
    for (int i = 0; i < len; i++) {
        buf[i] = padding_chars[rng_range(sizeof(padding_chars) - 1)];
    }
    buf[len] = '\0';
}

/* ================================================================
 * WORKLOAD A: seq_scan - Large sequential table scan
 * ================================================================ */

#define SEQ_SCAN_ROWS 100000
#define SEQ_SCAN_PAD  200

static void setup_seq_scan(sqlite3 *db) {
    char pad[SEQ_SCAN_PAD + 1];

    exec_or_die(db, "DROP TABLE IF EXISTS scan_test");
    exec_or_die(db, "CREATE TABLE scan_test(id INTEGER PRIMARY KEY, data TEXT)");
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "INSERT INTO scan_test(id, data) VALUES(?, ?)");

    for (int i = 1; i <= SEQ_SCAN_ROWS; i++) {
        fill_padding(pad, SEQ_SCAN_PAD);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_text(stmt, 2, pad, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
}

static double run_seq_scan(sqlite3 *db) {
    double t0 = now_secs();
    exec_or_die(db, "BEGIN");
    sqlite3_stmt *stmt = prepare_or_die(db, "SELECT * FROM scan_test");
    int rows = drain_rows(stmt);
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    double elapsed = now_secs() - t0;
    fprintf(stderr, "  seq_scan: %d rows in %.3fs\n", rows, elapsed);
    return elapsed;
}

/* ================================================================
 * WORKLOAD B: rev_scan - Reverse sequential scan
 * ================================================================ */

/* Reuses scan_test table from seq_scan setup */

static void setup_rev_scan(sqlite3 *db) {
    /* Table already created by seq_scan setup */
    (void)db;
}

static double run_rev_scan(sqlite3 *db) {
    double t0 = now_secs();
    exec_or_die(db, "BEGIN");
    sqlite3_stmt *stmt = prepare_or_die(db,
        "SELECT * FROM scan_test ORDER BY id DESC");
    int rows = drain_rows(stmt);
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    double elapsed = now_secs() - t0;
    fprintf(stderr, "  rev_scan: %d rows in %.3fs\n", rows, elapsed);
    return elapsed;
}

/* ================================================================
 * WORKLOAD C: idx_range - Index range scan
 * ================================================================ */

#define IDX_RANGE_ROWS 100000
#define IDX_RANGE_PAD  200

static void setup_idx_range(sqlite3 *db) {
    char pad[IDX_RANGE_PAD + 1];

    exec_or_die(db, "DROP TABLE IF EXISTS range_test");
    exec_or_die(db, "CREATE TABLE range_test(id INTEGER PRIMARY KEY, val INTEGER, data TEXT)");
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "INSERT INTO range_test(id, val, data) VALUES(?, ?, ?)");

    rng_seed(0xCAFEBABE);
    for (int i = 1; i <= IDX_RANGE_ROWS; i++) {
        fill_padding(pad, IDX_RANGE_PAD);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_int(stmt, 2, (int)rng_range(100000));
        sqlite3_bind_text(stmt, 3, pad, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    exec_or_die(db, "CREATE INDEX idx_range_val ON range_test(val)");
}

static double run_idx_range(sqlite3 *db) {
    double t0 = now_secs();
    exec_or_die(db, "BEGIN");
    sqlite3_stmt *stmt = prepare_or_die(db,
        "SELECT * FROM range_test WHERE val BETWEEN 1000 AND 5000 ORDER BY val");
    int rows = drain_rows(stmt);
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    double elapsed = now_secs() - t0;
    fprintf(stderr, "  idx_range: %d rows in %.3fs\n", rows, elapsed);
    return elapsed;
}

/* ================================================================
 * WORKLOAD D: point_lookup - Random point lookups
 * ================================================================ */

#define POINT_LOOKUP_ROWS   100000
#define POINT_LOOKUP_PAD    200
#define POINT_LOOKUP_QUERIES 10000

static void setup_point_lookup(sqlite3 *db) {
    char pad[POINT_LOOKUP_PAD + 1];

    exec_or_die(db, "DROP TABLE IF EXISTS point_test");
    exec_or_die(db, "CREATE TABLE point_test(id INTEGER PRIMARY KEY, data TEXT)");
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "INSERT INTO point_test(id, data) VALUES(?, ?)");

    for (int i = 1; i <= POINT_LOOKUP_ROWS; i++) {
        fill_padding(pad, POINT_LOOKUP_PAD);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_text(stmt, 2, pad, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
}

static double run_point_lookup(sqlite3 *db) {
    double t0 = now_secs();
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "SELECT * FROM point_test WHERE id = ?");

    rng_seed(0xDEADBEEF);
    int hits = 0;
    for (int i = 0; i < POINT_LOOKUP_QUERIES; i++) {
        int id = 1 + (int)rng_range(POINT_LOOKUP_ROWS);
        sqlite3_bind_int(stmt, 1, id);
        if (sqlite3_step(stmt) == SQLITE_ROW) hits++;
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    double elapsed = now_secs() - t0;
    fprintf(stderr, "  point_lookup: %d/%d hits in %.3fs\n",
            hits, POINT_LOOKUP_QUERIES, elapsed);
    return elapsed;
}

/* ================================================================
 * WORKLOAD E: btree_probe - Repeated index probes by TEXT key
 * ================================================================ */

#define BTREE_PROBE_ROWS    100000
#define BTREE_PROBE_PAD     200
#define BTREE_PROBE_QUERIES 10000

static void setup_btree_probe(sqlite3 *db) {
    char pad[BTREE_PROBE_PAD + 1];
    char key[32];

    exec_or_die(db, "DROP TABLE IF EXISTS btree_test");
    exec_or_die(db, "CREATE TABLE btree_test(id INTEGER PRIMARY KEY, key TEXT, val TEXT)");
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "INSERT INTO btree_test(id, key, val) VALUES(?, ?, ?)");

    for (int i = 1; i <= BTREE_PROBE_ROWS; i++) {
        snprintf(key, sizeof(key), "key_%08d", i);
        fill_padding(pad, BTREE_PROBE_PAD);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_text(stmt, 2, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, pad, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    exec_or_die(db, "CREATE INDEX idx_btree_key ON btree_test(key)");
}

static double run_btree_probe(sqlite3 *db) {
    char key[32];
    double t0 = now_secs();
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "SELECT * FROM btree_test WHERE key = ?");

    rng_seed(0xFEEDFACE);
    int hits = 0;
    for (int i = 0; i < BTREE_PROBE_QUERIES; i++) {
        int id = 1 + (int)rng_range(BTREE_PROBE_ROWS);
        snprintf(key, sizeof(key), "key_%08d", id);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) hits++;
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
    double elapsed = now_secs() - t0;
    fprintf(stderr, "  btree_probe: %d/%d hits in %.3fs\n",
            hits, BTREE_PROBE_QUERIES, elapsed);
    return elapsed;
}

/* ================================================================
 * WORKLOAD F: mixed_rw - Interleaved reads and writes
 * ================================================================ */

#define MIXED_RW_ROWS   10000
#define MIXED_RW_PAD    200
#define MIXED_RW_ITERS  1000

static void setup_mixed_rw(sqlite3 *db) {
    char pad[MIXED_RW_PAD + 1];

    exec_or_die(db, "DROP TABLE IF EXISTS mixed_test");
    exec_or_die(db, "CREATE TABLE mixed_test(id INTEGER PRIMARY KEY, counter INTEGER, data TEXT)");
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "INSERT INTO mixed_test(id, counter, data) VALUES(?, 0, ?)");

    for (int i = 1; i <= MIXED_RW_ROWS; i++) {
        fill_padding(pad, MIXED_RW_PAD);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_text(stmt, 2, pad, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
}

static double run_mixed_rw(sqlite3 *db) {
    double t0 = now_secs();

    sqlite3_stmt *sel = prepare_or_die(db,
        "SELECT counter, data FROM mixed_test WHERE id = ?");
    sqlite3_stmt *upd = prepare_or_die(db,
        "UPDATE mixed_test SET counter = counter + 1 WHERE id = ?");

    rng_seed(0xBAADF00D);
    for (int i = 0; i < MIXED_RW_ITERS; i++) {
        int id1 = 1 + (int)rng_range(MIXED_RW_ROWS);
        int id2 = 1 + (int)rng_range(MIXED_RW_ROWS);
        int id3 = 1 + (int)rng_range(MIXED_RW_ROWS);

        exec_or_die(db, "BEGIN");

        /* Read */
        sqlite3_bind_int(sel, 1, id1);
        sqlite3_step(sel);
        sqlite3_reset(sel);

        /* Write */
        sqlite3_bind_int(upd, 1, id2);
        sqlite3_step(upd);
        sqlite3_reset(upd);

        /* Read */
        sqlite3_bind_int(sel, 1, id3);
        sqlite3_step(sel);
        sqlite3_reset(sel);

        exec_or_die(db, "COMMIT");
    }

    sqlite3_finalize(sel);
    sqlite3_finalize(upd);
    double elapsed = now_secs() - t0;
    fprintf(stderr, "  mixed_rw: %d iterations in %.3fs\n", MIXED_RW_ITERS, elapsed);
    return elapsed;
}

/* ================================================================
 * WORKLOAD G: repeated_scan - Same table scanned multiple times
 * ================================================================ */

#define REPEATED_SCAN_ROWS  50000
#define REPEATED_SCAN_PAD   200
#define REPEATED_SCAN_ITERS 5

static void setup_repeated_scan(sqlite3 *db) {
    char pad[REPEATED_SCAN_PAD + 1];

    exec_or_die(db, "DROP TABLE IF EXISTS repeat_test");
    exec_or_die(db, "CREATE TABLE repeat_test(id INTEGER PRIMARY KEY, data TEXT)");
    exec_or_die(db, "BEGIN");

    sqlite3_stmt *stmt = prepare_or_die(db,
        "INSERT INTO repeat_test(id, data) VALUES(?, ?)");

    rng_seed(0xC0FFEE42);
    for (int i = 1; i <= REPEATED_SCAN_ROWS; i++) {
        fill_padding(pad, REPEATED_SCAN_PAD);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_text(stmt, 2, pad, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    exec_or_die(db, "COMMIT");
}

static double run_repeated_scan(sqlite3 *db) {
    const char *patterns[] = {
        "%aBC%", "%XyZ%", "%mNO%", "%qRS%", "%JKL%"
    };
    double t0 = now_secs();

    for (int i = 0; i < REPEATED_SCAN_ITERS; i++) {
        exec_or_die(db, "BEGIN");
        sqlite3_stmt *stmt = prepare_or_die(db,
            "SELECT count(*) FROM repeat_test WHERE data LIKE ?");
        sqlite3_bind_text(stmt, 1, patterns[i], -1, SQLITE_STATIC);
        sqlite3_step(stmt);
        int cnt = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
        exec_or_die(db, "COMMIT");
        fprintf(stderr, "  repeated_scan iter %d: %d matches\n", i, cnt);
    }

    double elapsed = now_secs() - t0;
    fprintf(stderr, "  repeated_scan: %d iterations in %.3fs\n",
            REPEATED_SCAN_ITERS, elapsed);
    return elapsed;
}

/* ================================================================
 * Workload dispatch table
 * ================================================================ */

typedef struct {
    const char *name;
    void (*setup)(sqlite3 *);
    double (*run)(sqlite3 *);
    int needs_prior_setup; /* 1 if depends on another workload's setup */
} Workload;

/* NOTE: rev_scan reuses scan_test from seq_scan, so seq_scan must be set up first */
static Workload workloads[] = {
    {"seq_scan",      setup_seq_scan,      run_seq_scan,      0},
    {"rev_scan",      setup_rev_scan,      run_rev_scan,      1},
    {"idx_range",     setup_idx_range,     run_idx_range,     0},
    {"point_lookup",  setup_point_lookup,  run_point_lookup,  0},
    {"btree_probe",   setup_btree_probe,   run_btree_probe,   0},
    {"mixed_rw",      setup_mixed_rw,      run_mixed_rw,      0},
    {"repeated_scan", setup_repeated_scan, run_repeated_scan, 0},
    {NULL, NULL, NULL, 0}
};

static Workload *find_workload(const char *name) {
    for (int i = 0; workloads[i].name; i++) {
        if (strcmp(workloads[i].name, name) == 0)
            return &workloads[i];
    }
    return NULL;
}

/* ---------- Median helper ---------- */

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double median(double *arr, int n) {
    qsort(arr, n, sizeof(double), cmp_double);
    if (n % 2 == 1)
        return arr[n / 2];
    return (arr[n / 2 - 1] + arr[n / 2]) / 2.0;
}

/* ================================================================
 * Main
 * ================================================================ */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s <db_path> setup <workload|all>\n"
        "  %s <db_path> run <workload|all> [iterations]\n"
        "\nWorkloads: seq_scan rev_scan idx_range point_lookup "
        "btree_probe mixed_rw repeated_scan\n",
        prog, prog);
    exit(1);
}

int main(int argc, char **argv) {
    if (argc < 4) usage(argv[0]);

    const char *db_path = argv[1];
    const char *mode = argv[2];
    const char *wl_name = argv[3];
    int iterations = 3;
    if (argc >= 5) iterations = atoi(argv[4]);
    if (iterations < 1) iterations = 1;

    sqlite3 *db;
    int rc = sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database %s: %s\n", db_path, sqlite3_errmsg(db));
        return 1;
    }

    /* WAL mode is not supported by mvsqlite, use delete journal mode */
    exec_or_die(db, "PRAGMA journal_mode=DELETE");

    if (strcmp(mode, "setup") == 0) {
        if (strcmp(wl_name, "all") == 0) {
            for (int i = 0; workloads[i].name; i++) {
                fprintf(stderr, "Setting up: %s\n", workloads[i].name);
                workloads[i].setup(db);
            }
        } else {
            Workload *wl = find_workload(wl_name);
            if (!wl) {
                fprintf(stderr, "Unknown workload: %s\n", wl_name);
                return 1;
            }
            fprintf(stderr, "Setting up: %s\n", wl->name);
            wl->setup(db);
        }
    } else if (strcmp(mode, "run") == 0) {
        if (strcmp(wl_name, "all") == 0) {
            for (int i = 0; workloads[i].name; i++) {
                Workload *wl = &workloads[i];
                double times[64];
                int n = iterations > 64 ? 64 : iterations;
                fprintf(stderr, "Running: %s (%d iterations)\n", wl->name, n);
                for (int j = 0; j < n; j++) {
                    /* Close and reopen DB between iterations and between
                     * workloads to get a fresh page cache each time. */
                    sqlite3_close(db);
                    rc = sqlite3_open(db_path, &db);
                    if (rc != SQLITE_OK) {
                        fprintf(stderr, "Cannot reopen database: %s\n",
                                sqlite3_errmsg(db));
                        return 1;
                    }
                    exec_or_die(db, "PRAGMA journal_mode=DELETE");
                    times[j] = wl->run(db);
                }
                /* Print metrics from the last iteration (db still open) */
                print_prefetch_metrics(db, wl->name);
                double med = median(times, n);
                printf("RESULT %s %.6f %.6f %.6f\n",
                       wl->name, med, times[0], times[n - 1]);
                fflush(stdout);
            }
        } else {
            Workload *wl = find_workload(wl_name);
            if (!wl) {
                fprintf(stderr, "Unknown workload: %s\n", wl_name);
                return 1;
            }
            double times[64];
            int n = iterations > 64 ? 64 : iterations;
            fprintf(stderr, "Running: %s (%d iterations)\n", wl->name, n);
            for (int j = 0; j < n; j++) {
                sqlite3_close(db);
                rc = sqlite3_open(db_path, &db);
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "Cannot reopen database: %s\n",
                            sqlite3_errmsg(db));
                    return 1;
                }
                exec_or_die(db, "PRAGMA journal_mode=DELETE");
                times[j] = wl->run(db);
            }
            print_prefetch_metrics(db, wl->name);
            double med = median(times, n);
            printf("RESULT %s %.6f %.6f %.6f\n",
                   wl->name, med, times[0], times[n - 1]);
            fflush(stdout);
        }
    } else {
        usage(argv[0]);
    }

    sqlite3_close(db);
    return 0;
}
