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
  per-key write conflict).
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

## Path 3: PLCC Single-Phase — Bug

### Mechanism

PLCC (Page-Level Conflict Check) is designed to allow concurrent writes to
*different* pages within the same namespace, using finer-grained conflict
detection than the namespace-level LWV.

- **LWV read** (`commit.rs:220-244`): When `plcc_enable_ns=true`:
  - If `allow_skip_idempotency_check=true`: the LWV read is skipped entirely
    (line 220 condition is false).
  - If `allow_skip_idempotency_check=false`: LWV is read with **snapshot=true**
    (`commit.rs:222`, second parameter is `plcc_enable_ns=true`). Snapshot reads
    do **not** add to the read conflict set. Furthermore, even if
    `client_assumed_version < actual_last_write_version`, the code does **not**
    return `Conflict` when `plcc_enable_ns=true` (line 238-240).
  - In both sub-cases, **LWV is not in the read conflict set**.
- **LWV write** (`commit.rs:249-253`): Always written with
  `SetVersionstampedValue`. This adds LWV to the write conflict set, but since
  no PLCC transaction has LWV in its read conflict set, this is inert.
- **PLCC version check** (`commit.rs:257-286`): For each page P in the client's
  read set, read the latest version of P. If version > `client_assumed_version`,
  return `Conflict`. This catches writes committed before the transaction's read
  version.
- **PLCC read conflict range** (`delta/reader.rs:85-94`): For each page P in
  the read set where a version exists, adds read conflict range
  `[page_key(P, V_current), page_key(P, [0xff;10]) || 0x00)`. This is intended
  to catch writes between the read version and commit time.
- **Page index write** (`commit.rs:300-304`):
  `atomic_op(SetVersionstampedKey)` using a template key
  `page_key(P, [0u8;10]) || offset_bytes`. FDB adds this key to the **write
  conflict set**.

### The Problem

The write conflict key from `SetVersionstampedKey` is the **template key** —
`page_key(P, [0u8; 10])` followed by a 4-byte LE offset — not the resolved key
with the actual committed versionstamp. This template key sorts **below** the
PLCC read conflict range for any existing page:

```
Write conflict key:  prefix | ns_id | 'p' | page_index | 00 00 00 00 00 00 00 00 00 00 | XX XX XX XX
                                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                                          template zeros + 4-byte offset

PLCC read conflict:  [prefix | ns_id | 'p' | page_index | V_current ... ,
                      prefix | ns_id | 'p' | page_index | FF FF FF FF FF FF FF FF FF FF | 00)
                                                           ^^^^^^^^^^
                                                           V_current > [0u8; 10]
```

Since `[0u8; 10] < V_current` for any existing page, the write conflict key is
strictly less than the start of the PLCC read conflict range. FDB's conflict
check — `T.read_conflict ∩ C.write_conflict` — finds no overlap.

No other key in the transaction's write conflict set compensates:
- LWV: in write conflict, but never in PLCC read conflict (snapshot read or
  skipped entirely).
- `ci_key(hash)`: in write conflict (from `SetVersionstampedValue`), but PLCC
  read conflict covers page keys, not content index keys. And ci_keys are
  per-hash — two transactions writing different content to the same page have
  different ci_keys.
- `content_key`, `delta_referrer_key`: in write conflict (from `txn.set` in
  `apply_write`), but in a different key space (`'c'` / `'r'` prefix vs `'p'`).

### Counterexample

**Setup:** Namespace N with page P=0 at version V0, content hash H0. Two
clients A and B both read page 0 at version V0.

**Transaction T_A** (PLCC, `allow_skip_idempotency_check=true`):
- `client_assumed_version = V0`, `read_set = {0}`, writes page 0 → hash H_A.

**Transaction T_B** (PLCC, `allow_skip_idempotency_check=true`):
- `client_assumed_version = V0`, `read_set = {0}`, writes page 0 → hash H_B.

**Execution (T_A commits first):**

1. T_A and T_B both begin with read versions RV_A ≈ RV_B ≈ V0 (before either
   commits). Both skip the LWV read (line 220 is false).

2. T_A's PLCC check: latest version of page 0 = V0. V0 ≤ V0 → no version
   conflict. Adds read conflict `[page_key(0, V0), page_key(0, [0xff;10]) || 0x00)`.

3. T_A commits at version V_A > V0. Page 0 now has version V_A.

4. T_B's PLCC check: read version < V_A, so T_B sees latest version = V0.
   V0 ≤ V0 → no version conflict. Adds read conflict
   `[page_key(0, V0), page_key(0, [0xff;10]) || 0x00)`.

5. T_B commits. FDB checks T_B.read_conflict ∩ T_A.write_conflict:
   - T_A.write_conflict includes `page_key(0, [0u8;10]) || offset` (from
     `SetVersionstampedKey`), `ci_key(H_A)`, and `LWV`.
   - `page_key(0, [0u8;10]) || offset` < `page_key(0, V0)` → not in T_B's read
     range.
   - `ci_key(H_A)` is in content index key space → not in T_B's read range.
   - `LWV` is not in T_B's read conflict (PLCC skipped LWV read).
   - **No conflict detected.**

6. T_B commits at V_B. Page 0 now has versions V0, V_A, V_B. **T_B's write
   overwrites T_A's without detecting the conflict. Lost update.**

### Caveat: FDB Versionstamp Conflict Resolution

This analysis assumes FDB uses the **template key** (pre-substitution, with
zero bytes and offset suffix) for write conflict ranges. If FDB's commit proxy
resolves the versionstamp *before* computing write conflict ranges (using the
resolved key `page_key(P, V_committed)`), the bug would not exist — the
resolved key would fall within the PLCC read conflict range.

However, the multi-phase code pattern provides strong circumstantial evidence
that the template key is used:

1. `NextWriteNoWriteConflictRange` is set before each `SetVersionstampedKey`
   write in multi-phase (`commit.rs:297-298`). This option is pointless if the
   write conflict is already using the resolved key (which would be correct).
2. Explicit broad write conflict ranges are added afterwards
   (`commit.rs:311-319`). This compensates for the suppressed per-key conflicts,
   providing a range that covers all page versions.

If the default write conflict used the resolved key, the multi-phase code would
not need `NextWriteNoWriteConflictRange` + explicit broad ranges — the
per-key resolved conflicts would already be correct. The fact that this
compensation exists implies the default is insufficient, confirming the template
key hypothesis.

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

## Fix

The multi-phase path already demonstrates the correct pattern: suppress the
default write conflict from `SetVersionstampedKey` with
`NextWriteNoWriteConflictRange`, then add an explicit write conflict range that
covers the right key space.

The fix applies the same pattern to single-phase commits, but scoped per-page
instead of per-namespace to preserve PLCC's fine-grained semantics:

1. **Always** set `NextWriteNoWriteConflictRange` before the
   `SetVersionstampedKey` page write (not just for multi-phase). The template
   key write conflict is useless in all paths.

2. **Multi-phase** (unchanged): Add a single broad write conflict range covering
   all pages in the namespace.

3. **Single-phase** (new): For each written page P, add a write conflict range
   `[page_key(P, [0u8;10]), page_key(P, [0xff;10]) || 0x00)` covering all
   versions of P. This overlaps with the PLCC read conflict range
   `[page_key(P, V_current), page_key(P, [0xff;10]) || 0x00)` for any existing
   version V_current.

This is correct because:
- Two PLCC transactions writing to the **same** page now conflict via the
  per-page write conflict range overlapping the other's PLCC read conflict
  range.
- Two PLCC transactions writing to **different** pages don't conflict — their
  per-page write conflict ranges don't overlap with each other's read conflict
  ranges.
- Non-PLCC transactions still conflict on LWV as before; the per-page write
  conflict ranges are inert (no non-PLCC transaction has page-level read
  conflicts).

## Summary

| Path | Verdict | Mechanism |
|---|---|---|
| Non-PLCC single-phase | **Correct** | LWV non-snapshot read + atomic write = mutual conflict detection |
| Multi-phase | **Correct** | LWV + explicit broad write conflict ranges + commit token for GC |
| PLCC single-phase | **Bug (fixed)** | Per-page write conflict ranges replace useless template key conflicts |
