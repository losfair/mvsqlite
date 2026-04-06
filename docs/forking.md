# Namespace forking (overlay)

Namespace forking creates a lightweight, copy-on-write child namespace that
shares the page data of a parent (base) namespace at a fixed point in time.
Only pages written by the child are stored in the child; reads of unmodified
pages fall through to the base.

## Creating a fork

```
POST /api/create_namespace
{
  "key": "my_fork",
  "overlay_base": {
    "ns_id": "<hex-encoded 10-byte ns_id of the base namespace>",
    "snapshot_version": "<hex-encoded 10-byte version to pin the base at>"
  }
}
```

`snapshot_version` determines the point-in-time snapshot of the base that the
fork sees. It is typically the version returned by a recent read or commit
against the base namespace.

## Read path

When a page is read from a forked namespace:

1. The server looks for the page in the child namespace at the requested
   version.
2. If the page does not exist in the child (it was never written there), the
   server falls back to reading from the base namespace at
   `overlay_base.snapshot_version`.
3. The fallback read does a reverse range scan on the base's page index for the
   requested page, returning the latest version <= `snapshot_version`.

The child and base each store their own content (content-addressed by hash).
The child does not copy any data from the base at creation time.

## Write path

Writes go directly to the child namespace. Once a page is written in the
child, subsequent reads of that page are served from the child; the base is no
longer consulted for that page.

## Deletion

A base namespace cannot be deleted while it has overlay children. The server
returns **409 Conflict** in this case. Delete all child namespaces first, then
delete the base.

When a child namespace is deleted, its `overlay_ref` entry in the reverse index
is cleaned up atomically in the same transaction.

## Internal data model

### Namespace metadata (`nsmd`)

Each namespace stores a JSON metadata blob:

```json
{
  "lock": null,
  "overlay_base": {
    "ns_id": "0a1b2c3d4e5f6a7b8c9d",
    "snapshot_version": "00000190a5b3c7d2e1f0"
  },
  "truncated_before": "00000190a5b3c7d2e1f0"
}
```

- `overlay_base` — present on forked namespaces; identifies the base namespace
  and the pinned snapshot version.
- `truncated_before` — hex-encoded version watermark. Set by
  `truncate_versions` after a non-dry-run truncation. Reads at versions below
  this watermark are rejected. Fork creation with a `snapshot_version` below
  this watermark is rejected.

### Reverse index (`overlay_ref`)

A FoundationDB key-space that maps **base namespace -> child namespace**:

```
Key:   (metadata_prefix, "overlay_ref", 0x32, base_ns_id, child_ns_id)
Value: snapshot_version (10 bytes)
```

This index is maintained atomically:

- **On create**: written in the same FDB transaction that creates the child
  namespace, using `SetVersionstampedKey` so the child `ns_id` portion of the
  key is filled in by FDB's commit versionstamp.
- **On delete**: cleared in the same transaction that deletes the child
  namespace and its metadata.

The reverse index enables efficient lookup of all overlay children of a given
base namespace without scanning every namespace in the cluster.

## Interaction with garbage collection

Two GC operations exist for a namespace:

| Operation | What it does |
| --- | --- |
| `truncate_namespace` | Deletes old page versions, keeping only the latest version of each page below a cutoff (`before_version`). |
| `delete_unreferenced_content` | Deletes content blobs not referenced by any page entry. |

Both must be safe in the presence of overlay children.

### How `truncate_versions` is protected

`truncate_versions` clamps `before_version` so that it never exceeds the
minimum `snapshot_version` of any overlay child. This is enforced in two layers:

**Layer 1 -- initial clamping.** Before any pages are deleted, the server scans
the `overlay_ref` index for the target namespace and computes the minimum
`snapshot_version` across all children. `before_version` is reduced to at most
this value.

**Layer 2 -- per-batch conflict guard.** Each page-deletion batch transaction
performs a non-snapshot read of the `overlay_ref` range. This has two effects:

1. The read result is validated: if any child has `snapshot_version <
   before_version`, the truncation aborts.
2. The non-snapshot read adds the range to FDB's read conflict set. If a
   concurrent `create_namespace` commits a new `overlay_ref` entry between the
   transaction's read version and its commit, FDB's SSI (serializable snapshot
   isolation) causes the deletion transaction to conflict and retry.

### How `delete_unreferenced_content` is protected

`delete_unreferenced_content` builds a Bloom filter of all content hashes
referenced by page entries in the namespace, then deletes content not in the
filter.

As long as page entries needed by overlay children are preserved (guaranteed by
`truncate_versions` above), those entries remain in the page index, their
content hashes appear in the Bloom filter, and the corresponding content is not
deleted. No additional overlay-specific logic is needed in this path.

## Correctness analysis

### Safety property

> After any sequence of GC operations on a base namespace A, every overlay
> read from every child namespace B returns the same result as it would have
> without GC.

### Definitions

- A has page entries keyed by `(page_index, version)`, each storing a content
  hash.
- B has `overlay_base = { ns_id: A, snapshot_version: V_B }`.
- An **overlay read** of page P from B at version V_B performs a reverse scan
  of A's page entries for page P with version <= V_B, returning the latest
  match. Call this entry **E** with version **W**.
- `truncate_versions(A, before_version)` deletes non-latest page entries with
  version < before_version, keeping the latest version of each page below the
  cutoff.

### Proof that `truncate_versions` preserves overlay reads

After clamping, `before_version <= V_B` for every child B. We show E is never
deleted.

**Case 1: W >= before_version.** The deletion criterion requires
`this_version < before_version`, which is false. E is not deleted.

**Case 2: W < before_version <= V_B.** E is the latest version of page P with
version <= V_B. Because no version of P exists in (W, V_B], no version exists
in (W, before_version) either. Therefore E is the latest version of P with
version < before_version. The truncation logic explicitly preserves the latest
version of each page in the truncation range. E is not deleted.

In both cases E is preserved, so the overlay read returns the same result.

### Proof that `delete_unreferenced_content` preserves overlay reads

Since E is preserved (proven above), E's content hash H remains in A's page
index. The Bloom filter scan reads all page entries including E, so H is in the
filter. `delete_unreferenced_content` does not delete H.

### Concurrency: overlay children created during GC

Three timing cases for a child B whose `create_namespace` transaction commits
at time T_B, relative to a deletion batch transaction with read version R_D and
commit time T_D:

**Case A: T_B < R_D.** B's `overlay_ref` entry is visible to the batch's
non-snapshot read. If V_B < before_version, the validation check aborts the
truncation. Otherwise before_version <= V_B and the static proof applies.

**Case B: R_D <= T_B <= T_D.** B writes an `overlay_ref` entry that falls in
the deletion batch's read conflict range. FDB detects a write-read conflict and
the batch fails. The retry creates a new transaction, reads `overlay_ref`
again, and reduces to Case A.

**Case C: T_B > T_D.** The deletion batch committed before B existed. No
conflict is detected. The `truncated_before` watermark closes this gap (see
below).

### Truncation watermark (`truncated_before`)

Before any pages are deleted (non-dry-run), `truncate_versions` writes the
effective `before_version` into the namespace metadata as `truncated_before`,
taking the max of the existing watermark and the new value. Writing it before
deletions ensures that even if the process crashes mid-truncation, the
watermark is already in place. This watermark is then enforced at two points:

**Fork creation.** `create_namespace` with `overlay_base` reads the base
namespace's metadata inside the creation transaction. If
`snapshot_version < truncated_before`, the request is rejected with 409. This
closes Case C: even if the deletion batch committed before the fork creation
transaction started, the watermark is visible to the creation transaction and
the fork is rejected.

**Reads.** `handle_read_req` checks the namespace's `truncated_before` before
serving a page read. If the requested version is below the watermark, the read
fails with an error instead of returning silently wrong data. The same check
applies to overlay fallback reads against the base namespace.
