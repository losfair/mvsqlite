# Correctness Analysis of `nslock`

This document analyzes the namespace lock implemented in
`mvstore/src/nslock.rs`, with emphasis on interactions with transaction commit,
rollback, GC, and overlay namespaces.

## Claim

Subject to the assumptions below, `nslock` is correct:

1. Once `acquire_nslock` returns success, every later committed namespace write
   is either made by the matching `lock_owner`, or is rejected/conflicts.
2. `release_nslock(..., Commit)` removes the lock without losing committed
   owner writes.
3. `release_nslock(..., Rollback)` restores the namespace head to the lock's
   `snapshot_version`: page-index entries and changelog entries newer than the
   snapshot are removed, `last_write_version` is reset to the snapshot, and no
   transaction can commit into the rollback window after rollback has been
   marked.
4. Concurrent version truncation and content GC do not delete data needed by
   the locked snapshot or by the rolled-back namespace head.
5. Overlay children continue to read a stable base snapshot.

Assumptions:

- FoundationDB transactions provide serializable conflict checking, with
  non-snapshot reads and explicit conflict ranges behaving as expected.
- `NamespaceMetadataCache::get` is treated as a read of the namespace metadata
  key because it calls `add_single_key_read_conflict_range`, even though the
  underlying `txn.get` is snapshot-read.
- The lock owner string is a capability. If two clients intentionally use the
  same owner string, they jointly own the lock.
- Administrative destructive operations such as `delete_namespace` are outside
  the pessimistic write-lock contract. They can remove a locked namespace. This
  is an administrative override, not protection provided by `nslock`.

## State Protected by the Lock

The lock is stored in namespace metadata:

```rust
struct NamespaceLock {
    snapshot_version: String,
    owner: String,
    nonce: String,
    rolling_back: bool,
}
```

The protected mutable namespace state is:

- page-index keys `page(ns, page_index, version)`;
- the namespace `last_write_version` key;
- changelog keys;
- namespace metadata, including the lock and truncation watermark.

Content blobs and content-index entries are content-addressed staging data.
Rollback does not delete them immediately; `delete_unreferenced_content` reclaims
them after page references disappear.

## Fundamental Metadata Conflict Invariant

Every path that needs to honor a namespace lock reads namespace metadata through
`NamespaceMetadataCache::get`. That function:

1. obtains metadata, possibly from the cache;
2. explicitly adds the namespace metadata key to the transaction's read conflict
   set.

Every lock state transition writes the same metadata key through
`NamespaceMetadataCache::set`, which also updates FoundationDB metadata version.

Therefore, for any committing transaction T that validated lock state, and any
concurrent transaction L that changes lock state:

- if L commits between T's metadata read and T's commit, T conflicts;
- if T commits first, L observes either the old state or retries according to
  the surrounding loop.

This is the central reason commit, release, rollback marking, and rollback
validation compose correctly.

## Acquire

`acquire_nslock`:

1. reads namespace metadata;
2. rejects if another owner holds the lock;
3. reads `last_write_version` with `snapshot=false`;
4. for explicit versions, rejects rollback-ineligible snapshots;
5. chooses `snapshot_version`;
6. writes `metadata.lock = Some(...)`;
7. commits, retrying on conflict.

The non-snapshot `last_write_version` read is essential. A concurrent commit
writes `last_write_version` with `SetVersionstampedValue`. Thus an acquire that
chooses a snapshot from an old head conflicts if a commit reaches the namespace
before acquire commits.

Consider a commit C and acquire A racing on an unlocked namespace:

- If C reads metadata before A commits, then A's metadata write conflicts with
  C's metadata read if A commits first.
- If C commits first, C's `last_write_version` write conflicts with A's
  non-snapshot `last_write_version` read, so A retries and chooses a snapshot
  at least as new as C.

Thus a successful acquire linearizes at its metadata commit. Its
`snapshot_version` is not behind any commit that also successfully linearized
before the lock.

For explicit rollback targets, acquire additionally rejects a requested version
newer than the namespace head, older than `truncated_before`, or older than the
minimum overlay child snapshot for this namespace.

The overlay-child check is performed during acquire rather than only during
rollback release. This prevents a client from acquiring a lock that can never
be rolled back and then discovering the problem only at release time. The
overlay reference scan is non-snapshot, so a concurrent `create_namespace`
that writes an overlay reference conflicts with acquire if it races between the
scan and the metadata commit. If acquire commits first, the child creation
transaction conflicts on the base namespace metadata read or observes the lock
and is rejected.

## Commit While Locked

`Server::commit` checks the lock in phase 2, after any multi-phase staging:

```text
lock exists, matching owner, not rolling_back => proceed
lock exists, no owner                         => Conflict
lock exists, wrong owner                      => Gone
lock exists, rolling_back                     => Gone
no lock, lock_owner supplied                  => Gone
```

Because the metadata read is in the read conflict set, a transaction that saw a
valid lock cannot commit if the lock is removed or marked rolling back before
the transaction commits.

This holds for all commit modes:

- non-PLCC single-phase commits;
- PLCC single-phase commits;
- multi-phase commits.

In the multi-phase path, phase 1 may write content blobs and a commit token
before lock validation. That does not publish namespace data because page-index
entries, changelog entries, and `last_write_version` are written only in phase
2. If phase 2 fails the lock check, phase 1 leaves at most unreferenced content,
which content GC may later collect.

## Release In Commit Mode

Commit-mode release reads metadata, verifies the owner, rejects if rollback has
started, clears `metadata.lock`, and commits.

Race with an owner commit:

- If release commits before the owner commit, the owner commit's metadata read
  conflicts with the release metadata write.
- If the owner commit commits first, release can then remove the lock. The
  owner write remains published, which is exactly commit-mode release semantics.

Race with a non-owner commit:

- Before release commits, non-owner commits observe a lock and return
  `Conflict` or `Gone`.
- After release commits, normal optimistic commit rules apply.

Therefore commit-mode release does not admit a write in the locked interval by
any non-owner, and it does not erase an owner write that committed before the
release linearized.

## Release In Rollback Mode

Rollback has three logical phases.

### Phase 1: Mark Rolling Back

The first transaction verifies ownership, rechecks overlay children, sets
`lock.rolling_back = true`, clears the rollback cursor, and commits.

After this transaction commits, no transaction can start and successfully commit
as the owner:

- a later owner commit reads `rolling_back = true` and returns `Gone`;
- an owner commit that read the previous lock state conflicts on the metadata
  key if the mark transaction commits first;
- if the owner commit commits first, it is a pre-mark write and is intentionally
  included in rollback.

Non-owner commits are already blocked by the lock.

The mark phase keeps an overlay reverse-reference check even though acquire
already rejects rollback-ineligible explicit snapshots. This is a defensive
guard for locks acquired before the validation existed and for resumed
rollbacks. If any child namespace depends on the base at a snapshot newer than
the rollback target, rollback is rejected before any page-index entry is
deleted. This prevents changing what an existing overlay child reads from its
base.

### Phase 2: Delete Newer Page Versions

Rollback scans all page-index keys for the namespace and deletes entries whose
version is greater than `snapshot_version`.

Before each batch commit, `lock_is_still_valid` reads namespace metadata and
checks the lock nonce. This has two effects:

- if the lock was removed or replaced, rollback returns `410`;
- if metadata changes after validation but before the batch commit, the batch
  conflicts because the metadata key is in the read conflict set.

The scan itself uses snapshot reads. This is safe because, after the mark phase,
no new page-index entries can successfully commit under the lock. Any page entry
newer than the snapshot was either committed before the mark phase and will be
found by some scan batch, or was part of a transaction that loses the metadata
race and does not commit.

The rollback cursor is conservative. It may cause a restarted rollback to
rescan part of a page index, but rescanning is idempotent because clearing a key
that is already absent is harmless.

### Phase 3: Finalize

Finalization again validates the nonce, then in one transaction:

1. sets `last_write_version` to `snapshot_version`;
2. clears changelog keys newer than `snapshot_version`;
3. clears `metadata.lock`.

Because lock removal is atomic with the head/changelog repair, normal clients
cannot observe an unlocked namespace whose `last_write_version` still points
past the rollback target.

After finalization, old transactions whose assumed version is newer than the
rolled-back head are rejected by the commit path. The commit code checks
`client_assumed_version > effective_last_write_version` even on the PLCC fast
path, so a client that observed the pre-rollback head cannot publish a commit
as if that head still existed.

## Interaction With Version Truncation

`truncate_versions` deletes old page versions, but it first writes a
`truncated_before` watermark in a transaction that also reads:

- namespace metadata;
- overlay reverse references.

If a lock exists, the effective truncation cutoff is clamped to
`lock.snapshot_version`. Therefore truncation cannot delete page versions needed
to read the locked snapshot.

Races with acquire are covered by the metadata conflict invariant:

- If acquire commits first, truncation's metadata read conflicts and truncation
  retries, then sees the lock and clamps.
- If truncation commits first, acquire's metadata read conflicts and acquire
  retries, then sees `truncated_before` and rejects explicit lock targets before
  the watermark.

After the watermark transaction, page deletion preserves the latest page version
in the truncated range. Thus a rollback target at or after the watermark remains
readable.

## Interaction With Content GC

`delete_unreferenced_content` deletes content blobs only after checking page
references and delta references. Rollback removes page-index entries newer than
the target and intentionally leaves content blobs behind.

Content needed by the rolled-back head remains protected because:

- page-index entries at or before `snapshot_version` are not deleted by
  rollback;
- delta base references are tracked by the delta-referrer index;
- content GC's correctness argument uses these references, content-index
  versionstamps, commit tokens, and conflict ranges to avoid deleting content
  referenced by a committed page entry.

Content written after the rollback target may become unreferenced once rollback
deletes newer page-index entries. Deleting that content later is correct because
no surviving namespace head references it.

Multi-phase commit staging remains safe with rollback and GC. If phase 1 writes
content but phase 2 is blocked by the lock or by `rolling_back`, only
unreferenced staged content remains. That can cause a later retry to rewrite
content, but it does not publish corrupt page state.

## Interaction With Overlay Namespaces

Overlay children pin a base namespace at `overlay_base.snapshot_version`.
`nslock` preserves that invariant in three places:

- explicit-version acquire rejects a lock snapshot older than any existing
  overlay child snapshot;
- `create_namespace` rejects creating an overlay from a locked base namespace;
- rollback release rechecks existing overlay references and rejects a rollback
  target older than any child snapshot before marking `rolling_back`.

Version truncation also clamps to the minimum overlay child snapshot. Together,
these rules prevent rollback or GC from changing the base data visible to an
existing overlay child.

## Limitations

- Re-acquiring with the same owner is idempotent and does not change
  `snapshot_version`. A fresh owner should be used for each distinct rollback
  attempt.
- The owner string is not a lease or authenticated session. Shared owner strings
  mean shared write authority.
- `nslock` protects the normal data-plane commit protocol and the GC/overlay
  paths described here. It does not prevent administrative deletion of a
  namespace.

## Conclusion

The mechanism is correct for its intended contract. The proof relies on one
simple synchronization point, the namespace metadata key, plus the
`last_write_version` read during acquire. Commit paths must validate metadata
before publishing page-index entries, rollback must mark `rolling_back` before
deleting page versions, and GC must honor lock snapshots and overlay references.
The current implementation satisfies those requirements.
