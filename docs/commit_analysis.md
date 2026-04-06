# Correctness Analysis of Transaction Commit Logic

This document analyzes the correctness of `Server::commit` in
`mvstore/src/commit.rs` across all code paths.

**Correctness invariant (serializability):** If transaction T reads a set of
pages at version V and writes based on that read, no other committed transaction
may have modified any of those pages between V and T's commit, unless T is
rejected with `Conflict`.

## Code Path Taxonomy

Three variables determine the commit path:

| Variable | Condition | Effect |
|---|---|---|
| `multi_phase` | `num_total_writes >= 1000` | Two FDB transactions instead of one |
| `plcc_enable` | `!multi_phase && total_read_set_size <= 2000` | Page-level conflict check enabled |
| `plcc_enable_ns` | `plcc_enable && ns.use_read_set` | Per-namespace PLCC activation |

This yields three distinct paths:

1. **Non-PLCC single-phase** (`multi_phase=false`, `plcc_enable_ns=false`)
2. **Multi-phase** (`multi_phase=true`, implies `plcc_enable=false`)
3. **PLCC single-phase** (`multi_phase=false`, `plcc_enable_ns=true`)

## Key FDB Behavior: SetVersionstampedKey Write Conflict Ranges

A critical detail for understanding the PLCC path is how FDB handles write
conflict ranges for `SetVersionstampedKey` atomic operations.

FDB does **not** add the raw template key to the write conflict set. Instead,
the ReadYourWrites layer (`ReadYourWrites.actor.cpp:2189-2203`) intercepts
`SetVersionstampedKey`, disables the default conflict range, and computes a
**bounding range** via `getVersionstampKeyRange()` (`Atomic.h:268-290`):

- **Lower bound:** The template key with the versionstamp bytes filled with
  `(readVersion + 1, batch=0)` — the minimum possible commit version.
- **Upper bound:** The template key with the versionstamp bytes filled with
  `\xFF` repeated 10 times — the maximum possible value.

For a page write to page P in a transaction with read version RV, this produces:

```
Write conflict range: [page_key(P, RV+1), page_key(P, [0xff;10]) || 0x00)
```

This range is conservative: it covers every possible committed versionstamp,
which is always >= RV+1.

### Why multi-phase uses NextWriteNoWriteConflictRange

Multi-phase commits (`commit.rs:297-309`) suppress the FDB-computed per-page
write conflict ranges with `NextWriteNoWriteConflictRange` and replace them with
a single broad write conflict range per namespace (`commit.rs:311-329`). This is
an **efficiency optimization**: instead of N individual per-page ranges for a
large transaction, one namespace-wide range is more compact. It is not a
correctness fix — the per-page ranges would also be correct.

## Path 1: Non-PLCC Single-Phase — Correct

### Mechanism

Conflict detection uses the **last-write-version (LWV)** key, a single key per
namespace that records the version of the most recent write.

- **Read** (`commit.rs:222`): `txn.get(&lwv_key, false)` — non-snapshot read,
  adds LWV to the transaction's **read conflict set**.
- **Version check** (`commit.rs:237-240`): If `client_assumed_version <
  actual_last_write_version` and `!plcc_enable_ns`, return `Conflict`. This
  catches writes committed before the transaction's read version.
- **Write** (`commit.rs:249-253`): `atomic_op(SetVersionstampedValue)` on LWV.
  Atomic operations add the key to the **write conflict set**.

### Proof

Let T1 and T2 be two non-PLCC transactions to the same namespace, both with
`client_assumed_version = V0`.

**Writes before read version:** If T1 committed at V1 where V1 is before T2's
read version, then T2 reads `actual_last_write_version = V1 > V0` at line 222
and returns `Conflict` at line 239.

**Writes between read version and commit time:** If T1 commits at V1 where
T2.read_version < V1 < T2.commit_time:
- LWV is in T2's read conflict set (from the non-snapshot get).
- LWV is in T1's write conflict set (from the atomic_op).
- FDB detects: T2.read_conflict ∩ T1.write_conflict = {LWV} ≠ ∅.
- T2 gets a transaction conflict from FDB.

Both windows are covered. **Correct. ✓**

## Path 2: Multi-Phase — Correct

### Mechanism

Phase 1 (first FDB transaction):
- Runs `WriteApplier` (content writes).
- Checks page existence for referenced content hashes.
- Sets a random commit token per namespace.
- Commits at version V_phase1.

Phase 2 (second FDB transaction, `set_read_version(V_phase1)`):
- Verifies commit tokens are unchanged (`commit.rs:165-173`).
- Reads LWV non-snapshot (`commit.rs:222`, since `plcc_enable_ns=false`).
- Checks `client_assumed_version < actual_last_write_version` → `Conflict`.
- Writes page index entries with `NextWriteNoWriteConflictRange` (suppresses
  per-key write conflict for efficiency).
- Adds explicit **broad write conflict ranges** covering the entire page key
  space and entire content index key space for the namespace
  (`commit.rs:311-329`).
- Writes LWV with `SetVersionstampedValue` (adds to write conflict).
- Commits.

### Proof

**Two multi-phase transactions T1, T2 to the same namespace:**

If T1 Phase 2 commits before T2 Phase 2:
- T2.read_conflict includes LWV (non-snapshot read at line 222).
- T1.write_conflict includes LWV (from atomic_op, no
  `NextWriteNoWriteConflictRange` for LWV) and the broad page/contentindex
  ranges.
- FDB detects the overlap on LWV. T2 Phase 2 conflicts. ✓

**Phase 1 / Phase 2 interleaving:**

If T1 Phase 1 commits between T2's Phase 2 read version and Phase 2 commit:
- T1 Phase 1 writes content keys (`txn.set`) and commit token (`txn.set`),
  which go into T1's write conflict set.
- T2 Phase 2's read conflict includes LWV and namespace metadata, but not
  commit tokens or content keys.
- No conflict on Phase 1 alone. But T1 Phase 2 must also commit for T1's
  changes to take effect. If T1 Phase 2 commits, it conflicts with T2 Phase 2
  on LWV as shown above. ✓

**GC interaction:** Analyzed in `docs/gc_analysis.md`. The commit token and
content index write conflict ranges ensure mutual exclusion between GC and
Phase 2. ✓

## Path 3: PLCC Single-Phase — Correct

### Mechanism

PLCC (Page-Level Conflict Check) allows concurrent writes to *different* pages
within the same namespace, using finer-grained conflict detection than the
namespace-level LWV.

- **LWV read** (`commit.rs:220-244`): When `plcc_enable_ns=true`:
  - If `allow_skip_idempotency_check=true`: the LWV read is skipped entirely
    (line 220 condition is false).
  - If `allow_skip_idempotency_check=false`: LWV is read with **snapshot=true**
    (`commit.rs:222`, second parameter is `plcc_enable_ns=true`). Snapshot reads
    do **not** add to the read conflict set. Furthermore, even if
    `client_assumed_version < actual_last_write_version`, the code does **not**
    return `Conflict` when `plcc_enable_ns=true` (line 238-240).
  - In both sub-cases, **LWV is not in the read conflict set**. This is
    intentional — PLCC uses page-level, not namespace-level, conflict detection.
- **PLCC version check** (`commit.rs:257-286`): For each page P in the client's
  read set, read the latest version of P. If version > `client_assumed_version`,
  return `Conflict`. This catches writes committed before the transaction's read
  version.
- **PLCC read conflict range** (`delta/reader.rs:85-94`): For each page P in
  the read set where a version exists, adds read conflict range
  `[page_key(P, V_current), page_key(P, [0xff;10]) || 0x00)`. This catches
  writes between the read version and commit time.
- **Page index write** (`commit.rs:300-304`):
  `atomic_op(SetVersionstampedKey)`. FDB's ReadYourWrites layer computes a
  bounding write conflict range (see above):
  `[page_key(P, RV+1), page_key(P, [0xff;10]) || 0x00)`.

### Proof

Let T1 and T2 be two PLCC transactions to the same namespace, both with
`client_assumed_version = V0`, and both with page P in their read sets.

**Writes before read version:** If T1 committed at V1 before T2's read version,
T2's PLCC check reads the latest version of page P, finds V1 > V0, and returns
`Conflict` at line 282. ✓

**Writes between read version and commit time:** If T1 commits at V1 where
T2.read_version < V1 < T2.commit_time:
- T2's PLCC read conflict for P: `[page_key(P, V_current), page_key(P, [0xff;10]) || 0x00)`
  where V_current <= T2.read_version.
- T1's write conflict for P (FDB-computed): `[page_key(P, T1.RV+1), page_key(P, [0xff;10]) || 0x00)`.
- Since T1.RV+1 > V_current (T1's read version is at least as recent as the
  page's current version), T1's write conflict range starts within T2's read
  conflict range.
- Both ranges share the same upper bound. The overlap is non-empty.
- FDB detects: T2.read_conflict ∩ T1.write_conflict ≠ ∅. T2 conflicts. ✓

**Writes to different pages:** If T1 writes page P and T2 writes page Q (P ≠ Q),
and T2's read set does not include P:
- T2 has no read conflict range for P.
- T1's write conflict for P doesn't overlap with any of T2's read conflicts.
- No conflict. Both commit. ✓ (This is the intended PLCC benefit.)

**Correct. ✓**

## Other Commit Checks

### Idempotency Check

The idempotency mechanism (`commit.rs:224-235`) compares the request's
`idempotency_key` against the one stored in LWV. If they match, the commit is a
retry of an already-committed transaction — it returns the previous result
without re-executing. This is correct and independent of the conflict path.

In the PLCC path with `allow_skip_idempotency_check=true`, the idempotency
check is skipped entirely (line 220). This is safe because `allow_skip` is only
set for first attempts, not retries.

### CausalReadRisky

The transaction uses `CausalReadRisky` (`commit.rs:100`), which allows FDB to
return a slightly stale read version. The guard at line 105 ensures `txn_rv >=
client_assumed_version` — if the server's view is behind the client's, the
commit is rejected. This prevents committing based on a snapshot that doesn't
include writes the client has already observed. Combined with the LWV/PLCC
checks (which detect concurrent writes AT the read version), CausalReadRisky is
safe.

### Namespace Metadata and Lock Checks

Metadata is read with snapshot=true (`metadata.rs:65`) but the key is added to
the read conflict set explicitly (`metadata.rs:76`). This ensures that if the
namespace metadata (including lock state) changes between the read and commit,
the transaction conflicts. The lock ownership check (`commit.rs:186-205`)
correctly validates that:
- If a lock exists, the committer must own it.
- If no lock exists, the committer must not claim ownership.
- Rolling-back namespaces reject all commits.

### Content Existence Check

Referenced page hashes are checked for existence (`commit.rs:135-150`). Hashes
written in the same transaction (`fast_write_hashes`) are skipped. External
hashes are read with `snapshot=false` (adds to read conflict). If content is
deleted between read and commit (e.g., by GC), the transaction conflicts on the
content index key. This is correct.

## Summary

| Path | Verdict | Mechanism |
|---|---|---|
| Non-PLCC single-phase | **Correct** | LWV non-snapshot read + atomic write = mutual conflict detection |
| Multi-phase | **Correct** | LWV + explicit broad write conflict ranges + commit token for GC |
| PLCC single-phase | **Correct** | FDB-computed bounding range for `SetVersionstampedKey` overlaps PLCC read conflict ranges |
