#!/bin/bash
#
# prefetch_bench.sh - End-to-end prefetcher benchmark for mvsqlite
#
# Runs SQLite workloads through the full stack (SQLite -> mvsqlite -> mvstore -> FDB)
# with varying prefetch depths and reports a comparison table.
#
# Prerequisites:
#   - FoundationDB installed and running
#   - cargo build --release -p mvstore -p mvsqlite
#   - make -C mvsqlite-preload build-patched-sqlite3
#
# Usage:
#   bash res/ci/prefetch_bench.sh [--quick]
#
# Options:
#   --quick   Only test depth=0 and depth=8, single cache configuration

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------- Configuration ----------

QUICK_MODE=0
if [[ "${1:-}" == "--quick" ]]; then
    QUICK_MODE=1
    shift
fi

if [[ $QUICK_MODE -eq 1 ]]; then
    PREFETCH_DEPTHS=(0 8)
    PAGE_CACHE_SIZES=(5000)
    CONTENT_CACHE_SIZES=(0)
else
    PREFETCH_DEPTHS=(0 4 8 16 32)
    PAGE_CACHE_SIZES=(0 5000)
    CONTENT_CACHE_SIZES=(0 10000)
fi

if [[ $QUICK_MODE -eq 1 ]]; then
    NUM_RUNS=1
else
    NUM_RUNS=3
fi
MVSTORE_PORT=7000
ADMIN_PORT=7001
MVSTORE_PID=""

WORKLOADS=(seq_scan rev_scan idx_range point_lookup btree_probe mixed_rw repeated_scan)

# Output files
RESULTS_CSV="$ROOT_DIR/prefetch_bench_results.csv"
RESULTS_TXT="$ROOT_DIR/prefetch_bench_results.txt"

# ---------- Paths ----------

MVSTORE="$ROOT_DIR/target/release/mvstore"
PRELOAD_LIB="$ROOT_DIR/mvsqlite-preload/libmvsqlite_preload.so"
PATCHED_SQLITE="$ROOT_DIR/mvsqlite-preload/libsqlite3.so"
WORKLOAD_BIN="$ROOT_DIR/res/ci/prefetch_workloads"
SPEEDTEST1=""  # set during build phase

# ---------- Build / check ----------

check_prerequisites() {
    local missing=0

    if [[ ! -x "$MVSTORE" ]]; then
        echo "ERROR: mvstore not found at $MVSTORE"
        echo "  Run: cargo build --release -p mvstore"
        missing=1
    fi

    if [[ ! -f "$ROOT_DIR/target/release/libmvsqlite.a" ]]; then
        echo "ERROR: libmvsqlite.a not found"
        echo "  Run: cargo build --release -p mvsqlite"
        missing=1
    fi

    if ! command -v fdbcli &>/dev/null; then
        echo "ERROR: fdbcli not found. Install FoundationDB."
        missing=1
    fi

    if [[ $missing -eq 1 ]]; then
        exit 1
    fi

    # Ensure ports are free
    if lsof -ti :$MVSTORE_PORT -ti :$ADMIN_PORT &>/dev/null; then
        echo "WARNING: Ports $MVSTORE_PORT/$ADMIN_PORT in use, killing existing processes..."
        lsof -ti :$MVSTORE_PORT -ti :$ADMIN_PORT 2>/dev/null | xargs -r kill 2>/dev/null || true
        sleep 2
    fi
}

build_patched_sqlite() {
    echo "=== Building patched libsqlite3.so ==="

    local sqlite_dir="$ROOT_DIR/_bench_sqlite_src"

    # Download sqlite3.c if not already in mvsqlite-preload
    if [[ ! -f "$ROOT_DIR/mvsqlite-preload/sqlite3.c" ]]; then
        echo "Downloading SQLite amalgamation..."
        mkdir -p "$sqlite_dir"
        cd "$sqlite_dir"
        if [[ ! -f sqlite3.c ]]; then
            curl -fL -o sqlite.zip https://sqlite.org/2026/sqlite-amalgamation-3510300.zip
            unzip -o sqlite.zip
            cp sqlite-amalgamation-3510300/sqlite3.c .
        fi
        cp sqlite3.c "$ROOT_DIR/mvsqlite-preload/sqlite3.c"
        cd "$ROOT_DIR/mvsqlite-preload"
        patch -p0 sqlite3.c < sqlite-3510300.patch
    fi

    cd "$ROOT_DIR"
    make -C mvsqlite-preload build-patched-sqlite3
    echo "  Built: $PATCHED_SQLITE"
}

build_workload_binary() {
    echo "=== Building prefetch_workloads benchmark ==="
    gcc -O2 -o "$WORKLOAD_BIN" "$ROOT_DIR/res/ci/prefetch_workloads.c" \
        -I"$ROOT_DIR/mvsqlite-preload" \
        -L"$ROOT_DIR/mvsqlite-preload" \
        -Wl,-rpath,"$ROOT_DIR/mvsqlite-preload" \
        -lsqlite3 -lpthread -ldl -lm
    echo "  Built: $WORKLOAD_BIN"
}

build_speedtest1() {
    echo "=== Building speedtest1 ==="

    local sqlite_src="$ROOT_DIR/_bench_sqlite_repo"
    if [[ ! -d "$sqlite_src" ]]; then
        git clone --depth=1 --branch version-3.31.1 https://github.com/sqlite/sqlite "$sqlite_src"
        cd "$sqlite_src"
        git apply "$ROOT_DIR/res/ci/sqlite.patch"
    fi

    cd "$sqlite_src"
    gcc -O2 -o speedtest1 test/speedtest1.c \
        -I"$ROOT_DIR/mvsqlite-preload" \
        -L"$ROOT_DIR/mvsqlite-preload" \
        -Wl,-rpath,"$ROOT_DIR/mvsqlite-preload" \
        -lsqlite3 -lpthread -ldl -lm

    SPEEDTEST1="$sqlite_src/speedtest1"
    echo "  Built: $SPEEDTEST1"
    cd "$ROOT_DIR"
}

# ---------- mvstore lifecycle ----------

start_mvstore() {
    local content_cache_size="$1"
    echo "--- Starting mvstore (content_cache=$content_cache_size) ---"

    RUST_LOG=error "$MVSTORE" \
        --data-plane "127.0.0.1:$MVSTORE_PORT" \
        --admin-api "127.0.0.1:$ADMIN_PORT" \
        --metadata-prefix "mvbench-$(date +%s)" \
        --raw-data-prefix "b$(date +%s)" \
        --auto-create-namespace \
        --content-cache-size "$content_cache_size" &
    MVSTORE_PID=$!
    sleep 2

    if ! kill -0 "$MVSTORE_PID" 2>/dev/null; then
        echo "ERROR: mvstore failed to start"
        exit 1
    fi
}

stop_mvstore() {
    if [[ -n "$MVSTORE_PID" ]] && kill -0 "$MVSTORE_PID" 2>/dev/null; then
        echo "--- Stopping mvstore (PID $MVSTORE_PID) ---"
        kill "$MVSTORE_PID" || true
        wait "$MVSTORE_PID" 2>/dev/null || true
        MVSTORE_PID=""
        sleep 1
    fi
}

create_namespace() {
    local ns="$1"
    curl -sf "http://localhost:$ADMIN_PORT/api/create_namespace" \
        -d "{\"key\":\"$ns\",\"metadata\":\"\"}" >/dev/null 2>&1 || true
}

# ---------- Benchmark runners ----------

# Run speedtest1, return the TOTAL time in seconds
run_speedtest1() {
    local ns="$1"
    local depth="$2"
    local page_cache="$3"

    create_namespace "$ns"

    local output
    output=$(LD_LIBRARY_PATH="$ROOT_DIR/mvsqlite-preload" \
        MVSQLITE_DATA_PLANE="http://localhost:$MVSTORE_PORT" \
        MVSQLITE_PREFETCH_DEPTH="$depth" \
        MVSQLITE_PAGE_CACHE_SIZE="$page_cache" \
        RUST_LOG=error \
        "$SPEEDTEST1" "$ns" 2>&1) || true

    # Parse TOTAL line: "       TOTAL.......................................................   20.509s"
    local total
    total=$(echo "$output" | grep -oP 'TOTAL[. ]+\K[0-9]+\.[0-9]+(?=s)' || echo "0")
    echo "$total"
}

# Run custom workloads, output RESULT lines
run_custom_workloads() {
    local ns="$1"
    local depth="$2"
    local page_cache="$3"

    create_namespace "$ns"

    # Setup phase (always with depth=0 for fairness)
    LD_LIBRARY_PATH="$ROOT_DIR/mvsqlite-preload" \
        MVSQLITE_DATA_PLANE="http://localhost:$MVSTORE_PORT" \
        MVSQLITE_PREFETCH_DEPTH=0 \
        MVSQLITE_PAGE_CACHE_SIZE="$page_cache" \
        RUST_LOG=error \
        "$WORKLOAD_BIN" "$ns" setup all 2>&1 | sed 's/^/  [setup] /' >&2

    # Run phase with configured depth
    LD_LIBRARY_PATH="$ROOT_DIR/mvsqlite-preload" \
        MVSQLITE_DATA_PLANE="http://localhost:$MVSTORE_PORT" \
        MVSQLITE_PREFETCH_DEPTH="$depth" \
        MVSQLITE_PAGE_CACHE_SIZE="$page_cache" \
        RUST_LOG=error \
        "$WORKLOAD_BIN" "$ns" run all "$NUM_RUNS" 2>/dev/null
}

# ---------- Result collection ----------

declare -A RESULTS  # key: "cc:pc:depth:workload" -> value: seconds

collect_result() {
    local cc="$1" pc="$2" depth="$3" workload="$4" secs="$5"
    RESULTS["$cc:$pc:$depth:$workload"]="$secs"
    echo "$cc,$pc,$depth,$workload,$secs" >> "$RESULTS_CSV"
}

# ---------- Report generation ----------

print_table() {
    local cc="$1" pc="$2"

    echo ""
    echo "=== Content cache: $cc, Page cache: $pc ==="
    echo ""

    # Header
    printf "%-20s" "Workload"
    for depth in "${PREFETCH_DEPTHS[@]}"; do
        printf "%12s" "depth=$depth"
    done
    echo ""
    printf '%0.s-' {1..80}; echo ""

    # speedtest1 row
    printf "%-20s" "speedtest1"
    for depth in "${PREFETCH_DEPTHS[@]}"; do
        local val="${RESULTS[$cc:$pc:$depth:speedtest1]:-N/A}"
        if [[ "$val" != "N/A" ]]; then
            printf "%11.3fs" "$val"
        else
            printf "%12s" "N/A"
        fi
    done
    echo ""

    # Custom workload rows
    for wl in "${WORKLOADS[@]}"; do
        printf "%-20s" "$wl"
        for depth in "${PREFETCH_DEPTHS[@]}"; do
            local val="${RESULTS[$cc:$pc:$depth:$wl]:-N/A}"
            if [[ "$val" != "N/A" ]]; then
                printf "%11.6fs" "$val"
            else
                printf "%12s" "N/A"
            fi
        done
        echo ""
    done

    # Speedup table
    echo ""
    printf "%-20s" "Speedup vs depth=0"
    for depth in "${PREFETCH_DEPTHS[@]}"; do
        printf "%12s" "depth=$depth"
    done
    echo ""
    printf '%0.s-' {1..80}; echo ""

    local all_wl=("speedtest1" "${WORKLOADS[@]}")
    for wl in "${all_wl[@]}"; do
        printf "%-20s" "$wl"
        local base="${RESULTS[$cc:$pc:0:$wl]:-0}"
        for depth in "${PREFETCH_DEPTHS[@]}"; do
            local val="${RESULTS[$cc:$pc:$depth:$wl]:-0}"
            if [[ "$base" != "0" ]] && [[ "$val" != "0" ]] && [[ "$base" != "N/A" ]] && [[ "$val" != "N/A" ]]; then
                local speedup
                speedup=$(awk "BEGIN { printf \"%.2f\", $base / $val }")
                printf "%11sx" "$speedup"
            else
                printf "%12s" "N/A"
            fi
        done
        echo ""
    done
}

# ---------- Cleanup ----------

cleanup() {
    stop_mvstore
    echo "Benchmark complete."
}
trap cleanup EXIT

# ================================================================
# Main
# ================================================================

echo "========================================"
echo " mvsqlite Prefetcher Benchmark"
echo "========================================"
echo ""
echo "Configuration:"
echo "  Prefetch depths: ${PREFETCH_DEPTHS[*]}"
echo "  Page cache sizes: ${PAGE_CACHE_SIZES[*]}"
echo "  Content cache sizes: ${CONTENT_CACHE_SIZES[*]}"
echo "  Iterations per workload: $NUM_RUNS"
echo ""

check_prerequisites

# Build phase
build_patched_sqlite
build_workload_binary
build_speedtest1

# Initialize CSV
echo "content_cache,page_cache,prefetch_depth,workload,seconds" > "$RESULTS_CSV"

# Run benchmark matrix
for cc in "${CONTENT_CACHE_SIZES[@]}"; do
    start_mvstore "$cc"

    for pc in "${PAGE_CACHE_SIZES[@]}"; do
        echo ""
        echo "========================================"
        echo " Testing: content_cache=$cc, page_cache=$pc"
        echo "========================================"

        for depth in "${PREFETCH_DEPTHS[@]}"; do
            echo ""
            echo "--- depth=$depth ---"

            # speedtest1
            local_ns="st1-cc${cc}-pc${pc}-d${depth}"
            echo "Running speedtest1 (ns=$local_ns)..."
            st1_time=$(run_speedtest1 "$local_ns" "$depth" "$pc")
            echo "  speedtest1 TOTAL: ${st1_time}s"
            collect_result "$cc" "$pc" "$depth" "speedtest1" "$st1_time"

            # Custom workloads
            local_ns="wl-cc${cc}-pc${pc}-d${depth}"
            echo "Running custom workloads (ns=$local_ns)..."
            while IFS= read -r line; do
                if [[ "$line" == RESULT* ]]; then
                    # Parse: RESULT <name> <median> <min> <max>
                    wl_name=$(echo "$line" | awk '{print $2}')
                    wl_median=$(echo "$line" | awk '{print $3}')
                    echo "  $wl_name: ${wl_median}s"
                    collect_result "$cc" "$pc" "$depth" "$wl_name" "$wl_median"
                fi
            done < <(run_custom_workloads "$local_ns" "$depth" "$pc")
        done
    done

    stop_mvstore
done

# Print results
{
    echo ""
    echo "========================================"
    echo " RESULTS"
    echo "========================================"

    for cc in "${CONTENT_CACHE_SIZES[@]}"; do
        for pc in "${PAGE_CACHE_SIZES[@]}"; do
            print_table "$cc" "$pc"
        done
    done

    echo ""
    echo "Raw CSV: $RESULTS_CSV"
} | tee "$RESULTS_TXT"

echo ""
echo "Results saved to:"
echo "  CSV: $RESULTS_CSV"
echo "  Text: $RESULTS_TXT"
