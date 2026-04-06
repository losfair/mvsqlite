# Correctness Analysis of `delete_unreferenced_content`

This document proves the correctness of the garbage collection algorithm in
`mvstore/src/gc.rs`, specifically `delete_unreferenced_content`, with respect to
concurrent writes.

**Correctness invariant:** GC never deletes content that is referenced by a
committed page index entry or delta referrer. Equivalently, no reader ever
observes a dangling reference to deleted content.

## Algorithm Summary

The algorithm proceeds in three steps:

1. **Capture RAW-RV** (line 305): Obtain the cluster's current read version from
   a fresh FoundationDB transaction.

2. **Build Bloom filter** (lines 308-336): Scan all page index entries and delta
   referrer entries across multiple transactions (an intentionally inconsistent
   snapshot). Insert every referenced content hash into a Bloom filter.

3. **Scan and delete** (lines 339-468): Iterate over the content index. For each
   content hash H, apply a series of filters. If H survives all filters, delete
   it. The filters and deletion are structured as two logical transactions per
   batch:

   - **TXN1**: Scan a batch of content index entries. For each entry H:
     - **(3a)** Skip if H is in the Bloom filter (referenced by the scan).
     - **(3b)** Skip if H was written recently (within `GC_FRESH_PAGE_TTL_SECS`).
     - **(3c)** Skip if H's versionstamp > RAW-RV (written after our snapshot).
   - **TXN2** (reset from TXN1, read version set to TXN1's read version):
     - **(3e)** Add `contentindex(H)` to the read conflict set.
     - **(3f)** Clear `contentindex(H)`, `content(H)`, `delta_referrer(H)`.
     - **(3g)** Clear the namespace commit token.
     - Commit TXN2.

## Proof of Correctness

We must show that if any committed page index entry or delta referrer references
hash H at the time GC's TXN2 commits, that commit will not succeed.

All concurrent writes fall into one of three cases based on commit version
relative to RAW-RV and the commit protocol used.

### Case 1: Single-phase commit at V_w <= RAW-RV

Both the page index entry and the content index entry for H are written
atomically in one FoundationDB transaction at version V_w. Since V_w <= RAW-RV
and RAW-RV was obtained before the Bloom filter scan begins, every scan
transaction in Step 2 gets a read version >= RAW-RV >= V_w (FoundationDB
guarantees monotonically non-decreasing read versions within a client). The page
index entry is therefore visible to the scan, H is inserted into the Bloom
filter, and Step 3a skips it.

Bloom filters have false positives but never false negatives, so an inserted
hash is always found.

### Case 2: Any commit at V_w > RAW-RV

The content index entry for H has a versionstamp equal to V_w. Step 3c compares
this against RAW-RV:

```rust
let their_version = i64::from_be_bytes(ci.versionstamp[0..8].try_into().unwrap());
if their_version > read_version {
    continue;
}
```

Since V_w > RAW-RV, H is skipped. This covers all single-phase commits after
RAW-RV, and all multi-phase commits whose Phase 1 committed after RAW-RV.

### Case 3: Multi-phase commit with Phase 1 at V_1 <= RAW-RV, Phase 2 pending

This is the subtle case. After Phase 1, content H exists (with versionstamp
V_1 <= RAW-RV) but no page index entry references it yet. The Bloom filter does
not contain H, and Step 3c does not filter it. GC attempts deletion.

The two transactions (Phase 2 and GC-TXN2) race. Exactly one must conflict:

**If GC-TXN2 commits first (at V_gc):**

- GC clears the commit token (Step 3g), which is added to GC-TXN2's write
  conflict ranges.
- Phase 2 reads the commit token with `txn.get(k, false)` (`commit.rs:167`),
  adding it to Phase 2's read conflict ranges.
- Phase 2 detects a conflict on the commit token (V_1 < V_gc < Phase 2 commit)
  and aborts with "commit interrupted before phase 2."
- No page index entry was ever created. No dangling reference.

**If Phase 2 commits first (at V_2):**

- Phase 2 explicitly adds the entire content index range as a write conflict
  range (`commit.rs:321-329`), compensating for the `NextWriteNoWriteConflictRange`
  optimization on individual keys.
- GC-TXN2 has `contentindex(H)` in its read conflict set (Step 3e).
- GC-TXN2 detects a conflict (TXN2.RV < V_2 < V_gc) and fails. The content is
  preserved. GC logs the conflict and moves on (`gc.rs:459`).

These sub-cases are exhaustive and mutually exclusive. **QED.**

### Delta referrers

When content H is delta-encoded against base B, the write transaction
atomically writes `content(H)`, `delta_referrer(H) = B`, and updates
`contentindex(B)` with `SetVersionstampedValue` (`write.rs:171-180`). This
means B's versionstamp is refreshed to the write's commit version.

The same three-case analysis applies to B: either B is in the Bloom filter (via
the delta referrer scan), or its versionstamp is too recent, or the multi-phase
conflict machinery protects it.

## Role of the TTL Check (Step 3b)

The TTL check is **not necessary for the correctness invariant**. Its purpose is
to prevent GC from interfering with the `/batch/write` -> `/batch/commit` flow.

The `/batch/write` endpoint (`server.rs:1456-1534`) writes content in a
standalone transaction without creating page index entries or setting a commit
token. The page index entries are created later in a separate `/batch/commit`
request. Between these two calls, the content is unreferenced.

If GC runs during this window:

1. The Bloom filter does not contain H (no page index entry exists).
2. Step 3c does not filter H (the write committed before RAW-RV).
3. The commit token mechanism provides no protection (none was set).
4. GC deletes H.
5. The subsequent `/batch/commit` returns `BadPageReference`.

This is not a correctness violation: no committed page index entry ever
references deleted content, and no SQLite data structures are corrupted. The
client receives an error and can retry. The TTL prevents this **availability
degradation** by skipping recently-written content, giving clients time to
complete their commit. As the comment in the source states:

```rust
// This is not necessary for correctness, but removing this may cause transactions to fail.
```

## Protection Mechanisms Summary

| Mechanism | Protects against |
|---|---|
| Bloom filter + versionstamp (3a, 3c) | Deletion of content with a committed page reference |
| Commit token + FDB conflict detection (3e, 3g) | Race between GC and multi-phase commits |
| TTL (3b) | Spurious `BadPageReference` in the `/batch/write` -> `/batch/commit` flow |
