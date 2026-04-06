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

Each namespace stores a JSON metadata blob. Forked namespaces have an
`overlay_base` field:

```json
{
  "lock": null,
  "overlay_base": {
    "ns_id": "0a1b2c3d4e5f6a7b8c9d",
    "snapshot_version": "00000190a5b3c7d2e1f0"
  }
}
```

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
conflict is detected. This is a residual race window: if B's
`snapshot_version` falls within the already-truncated range, pages it depends
on may be gone.

### Residual race (Case C) and mitigation

Case C requires a client to create a fork with a `snapshot_version` older than
the GC's effective `before_version`, AND to commit strictly after the deletion
batch. In practice:

- `before_version` is clamped to the cluster's current read version, so it is
  roughly "now". A fork's `snapshot_version` that is older than this cutoff
  would reference data the operator has explicitly chosen to garbage-collect.
- The race window is a single FDB transaction lifetime (typically milliseconds).
  A fork creation that starts before GC and commits after would need to overlap
  the exact batch window.

A complete solution would require either a global serialization mechanism
between truncation and fork creation (e.g. a distributed lock or epoch counter),
or post-hoc validation in `create_namespace` that rejects fork creation when
the base has been truncated past the requested `snapshot_version`. The current
design accepts the residual risk as an acceptable trade-off for simplicity.
