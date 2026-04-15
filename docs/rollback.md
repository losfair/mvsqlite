# Rolling a Namespace Back to an Old Version

This document describes how a data-plane client can roll a namespace
(SQLite database) back to a previous mvSQLite version.

Rollback is implemented by the data-plane `nslock` mechanism:

1. Acquire an exclusive namespace lock at the target snapshot version.
2. Release that lock in `rollback` mode.

The rollback is destructive. It deletes namespace page-index entries newer
than the target version, resets the namespace last-write version to the target
version, deletes newer changelog entries, and removes the lock.

## When to Use This

Use this when the current namespace head should be moved back to an older
committed version.

Do not confuse this with SQLite transaction rollback. SQLite transaction
rollback only discards a single uncommitted transaction. Namespace rollback
changes the persisted mvSQLite namespace head.

## Prerequisites

- A writable mvstore data-plane endpoint, for example
  `http://localhost:7000`.
- The namespace key, for example `test`.
- A unique lock owner string, at most 256 bytes.
- The target version, as a 20-character hex-encoded 10-byte mvSQLite version.

If the namespace key uses hashproof protection, send both headers used by
normal data-plane clients:

```http
x-namespace-key: <namespace-key-containing-hash>
x-namespace-hashproof: <hex-proof>
```

For unprotected namespace keys, only `x-namespace-key` is required.

## Choosing the Target Version

Versions are hex-encoded FoundationDB versionstamps used by mvSQLite.

Common ways to get a candidate version:

- Save the `CommitResult.version` returned by `mvclient` after a successful
  commit.
- Call `GET /stat` against the namespace to get the current head version.
- Call `GET /time2version?t=<unix-seconds>` to map a wall-clock time to nearby
  versions.
- From SQLite, call `mv_last_known_version('main')` after a transaction or
  `mv_time2version('main', <unix-seconds>)`.

Example current namespace head:

```bash
DP=http://localhost:7000
NS=test

curl -sS "$DP/stat" \
  -H "x-namespace-key: $NS"
```

Example time lookup:

```bash
curl -sS "$DP/time2version?t=1710000000"
```

The response contains `after` and `not_after` points. `after` is the latest
recorded timekeeper point before the requested timestamp, and `not_after` is
the first point at or after it. For most "roll back to the database state
around this time" workflows, use `after.version`; this is also what the SQLite
`mv_time2version` helper returns.

Before performing a destructive rollback, verify the selected snapshot by
opening the namespace read-only at that version. In SQLite, either open
`<namespace>@<version>` or pin the connection:

```sql
select mv_pin_version('main', '<target-version>');
-- inspect the database
select mv_unpin_version('main');
```

Writes to a pinned/fixed version are discarded by the VFS.

## Rollback Procedure

Set variables:

```bash
DP=http://localhost:7000
NS=test
OWNER=rollback-$(hostname)-$(date +%s)
TARGET_VERSION=<20-char-hex-version>
```

Acquire the namespace lock at the target version:

```bash
curl -i -sS -X POST "$DP/nslock/acquire" \
  -H "content-type: application/json" \
  -H "x-namespace-key: $NS" \
  -d "{\"owner\":\"$OWNER\",\"version\":\"$TARGET_VERSION\"}"
```

Expected success status: `201 Created`.

Release the lock in rollback mode:

```bash
curl -i -sS -X POST "$DP/nslock/release" \
  -H "content-type: application/json" \
  -H "x-namespace-key: $NS" \
  -d "{\"owner\":\"$OWNER\",\"mode\":\"rollback\"}"
```

Expected success status: `200 OK`.

Verify that the lock is gone and that normal clients see the target version:

```bash
curl -sS "$DP/stat" \
  -H "x-namespace-key: $NS"
```

If the admin API is available, also verify namespace metadata:

```bash
ADMIN=http://localhost:7001

curl -sS -X POST "$ADMIN/api/stat_namespace" \
  -H "content-type: application/json" \
  -d "{\"key\":\"$NS\"}"
```

The metadata should have `"lock": null`.

## What Other Clients See

After `nslock/acquire` succeeds:

- Normal readers that do not pass `lock_owner` see the lock's
  `snapshot_version`, which is the target rollback version.
- Normal writers that do not pass `lock_owner` conflict while the lock exists.
- A client configured with the same lock owner can still stat and commit
  against the live namespace while the lock is held.

After `nslock/release` starts in `rollback` mode:

- The lock is marked `rolling_back`.
- The lock owner can no longer start or commit owned transactions.
- Normal readers continue to see the target snapshot.
- The release request scans page entries in batches and removes entries newer
  than the target version.

After rollback finishes:

- The lock is removed.
- The namespace last-write version is the target version.
- Normal readers and writers use the rolled-back namespace head.

## mvclient and SQLite Integration

`mvclient` does not currently expose a high-level rollback method. A client
that wants to roll back a namespace should issue the raw HTTP requests above.

`MultiVersionClientConfig.lock_owner` is still important for clients that need
to operate while holding a lock:

- It appends `lock_owner=<owner>` to `/stat` requests.
- It includes the owner in `/batch/commit`.

The SQLite VFS reads the same value from the environment:

```bash
export MVSQLITE_LOCK_OWNER="$OWNER"
```

For a pure rollback to an old version, do not perform writes between lock
acquisition and `rollback` release. Any page versions newer than the target are
removed by the rollback.

## Status Codes

### `POST /nslock/acquire`

| Status | Meaning |
| --- | --- |
| `201` | Lock acquired, or the same owner already owns it. |
| `400` | Invalid owner, usually empty or more than 256 bytes. |
| `404` | Namespace key was not found. |
| `409` | Another owner holds the lock. The `x-lock-owner` header identifies it. |

Important: acquire is idempotent only by owner. If the same owner already holds
the lock, another acquire returns `201` without changing the lock's
`snapshot_version`. Use a fresh unique owner for each rollback attempt, or
inspect namespace metadata before assuming the lock target changed.

### `POST /nslock/release`

| Status | Meaning |
| --- | --- |
| `200` | Rollback completed. |
| `201` | Commit-mode release completed. |
| `400` | Invalid owner. |
| `404` | Namespace key was not found. |
| `410` | Lock was lost during rollback. |
| `422` | Namespace is not locked, or is locked by a different owner. |

## Constraints and Caveats

- The target should be a known committed/readable version and should not be
  newer than the namespace head you intend to replace.
- The target version must still be readable. If the namespace has a
  `truncated_before` watermark newer than the target, normal reads at the
  rolled-back head will be rejected.
- Rollback moves one namespace head. It is not a multi-namespace atomic
  rollback operation.
- Rollback removes page-index entries and changelog entries newer than the
  target version. Content blobs and content-index entries may remain until
  storage cleanup runs.
- To reclaim storage after rollback, run the admin
  `delete_unreferenced_content` operation for the namespace.
- If a rollback request fails due to a network or server interruption, inspect
  `metadata.lock` with the admin `stat_namespace` endpoint before retrying or
  starting another rollback.

## Implementation Pointers

- Data-plane routes: `mvstore/src/server.rs`, paths `/nslock/acquire` and
  `/nslock/release`.
- Rollback implementation: `mvstore/src/nslock.rs`, `release_nslock`.
- Lock-aware stat behavior: `mvstore/src/stat.rs`.
- Lock-aware commit behavior: `mvstore/src/commit.rs`.
- Client lock-owner propagation: `mvclient/src/lib.rs`.
- SQLite environment variable: `MVSQLITE_LOCK_OWNER` in `mvsqlite/src/lib.rs`.
