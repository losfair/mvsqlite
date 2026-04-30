# nslock / lock_owner Mechanism

## Purpose

nslock provides **explicit pessimistic namespace-level locking** for mvSQLite. During normal operation, mvSQLite uses optimistic concurrency (transactions retry on conflict). nslock is an opt-in mechanism for scenarios needing exclusive write access with rollback capability.

## Core Data Structure

The lock is stored as part of namespace metadata in FoundationDB (`mvstore/src/metadata.rs`):

```rust
struct NamespaceLock {
    snapshot_version: String,  // hex version at lock acquisition time
    owner: String,             // client identifier (max 256 bytes)
    nonce: String,             // random 16-byte hex; prevents stale rollbacks
    rolling_back: bool,        // true during rollback phase
}
```

## Lock Lifecycle

### 1. Acquire

```
POST /nslock/acquire?ns=<hex>
Content-Type: application/json

{"owner": "my-id", "version": "optional-hex-version"}
```

- If already locked by **same owner**: 201 (idempotent).
- If locked by **different owner**: 409 with `x-lock-owner` header.
- Otherwise: records `snapshot_version` (current LWV or the explicit `version` parameter), generates a random `nonce`, and stores the lock in FDB metadata. Returns 201.

Implementation: `mvstore/src/nslock.rs` (`acquire_nslock`).

### 2. Use -- Stat and Commit with `lock_owner`

#### Stat (`GET /stat`)

When a namespace is locked (`mvstore/src/stat.rs`):

- **Lock owner** (passes `lock_owner` query param): gets the live LWV (sees own writes).
- **Other readers** (no `lock_owner`): get `snapshot_version` (frozen point-in-time, read-only MVCC snapshot).
- **Other writers claiming ownership**: `GoneError`.

#### Commit (`POST /batch/commit`)

For each namespace in a commit (`mvstore/src/commit.rs`):

| Lock exists? | `lock_owner` provided? | Result |
|---|---|---|
| Yes | Yes, matches | Commit proceeds |
| Yes | Yes, mismatch | `GoneError` |
| Yes | No | `Conflict` (blocks non-owners) |
| No | Yes | `GoneError` (lock was lost) |
| No | No | Normal commit |

### 3. Release

```
POST /nslock/release?ns=<hex>
Content-Type: application/json

{"owner": "my-id", "mode": "commit" | "rollback"}
```

#### Commit mode

Simply removes the lock. All writes persist. Returns 201.

#### Rollback mode

Three-phase process:

1. **Mark**: Sets `rolling_back = true` in the lock metadata and clears the rollback cursor.
2. **Scan & delete**: Iterates all pages in batches (default 1000, configurable via `--knob-nslock-rollback-scan-batch-size`). Deletes every page version newer than `snapshot_version`. Validates the nonce each batch to detect if the lock was stolen.
3. **Finalize**: Resets LWV to `snapshot_version`, deletes changelog entries after the snapshot point, and removes the lock.

If the nonce changes mid-rollback (lock lost and re-acquired by another client): 410 "lock is gone".

Implementation: `mvstore/src/nslock.rs` (`release_nslock`).

## Client-Side Configuration

There are two ways to surface the lock owner to the mvclient.

### 1. Environment variable (process-wide default)

The SQLite VFS layer (`mvsqlite/src/lib.rs`) reads the lock owner from the environment:

```bash
export MVSQLITE_LOCK_OWNER="my-unique-client-id"
```

When set, every `MultiVersionClient` produced by the VFS starts with this
value as its effective lock owner. The mvclient automatically:

- Appends `lock_owner=...` to `/stat` requests.
- Includes `lock_owner` in `CommitGlobalInit` during batch commit.

### 2. SQL functions (per-connection)

mvsqlite registers three SQL functions on every connection (see
`mvsqlite/src/lib.rs`, `init_mvsqlite_connection`):

```sql
SELECT mv_nslock_acquire('main', 'worker-1');                 -- live LWV
SELECT mv_nslock_acquire('main', 'worker-1', '<version-hex>'); -- explicit version

SELECT mv_nslock_release_commit('main', 'worker-1');
SELECT mv_nslock_release_rollback('main', 'worker-1');
```

The first argument is the attached schema name (`'main'` for the default
database). All three are `SQLITE_DIRECTONLY` and return `NULL` on success or
a SQL error on failure.

A successful `mv_nslock_acquire` updates the **calling connection's**
effective lock owner; subsequent `/stat` and `/batch/commit` requests issued
by that connection carry the new `lock_owner`. A successful release clears
it. The change is scoped to the single mvclient instance backing that
connection and does not affect sibling connections sharing the same VFS.

Both the env-var and SQL paths share the same wire protocol; the SQL path is
preferred when a process needs to acquire the lock at runtime, scope the
owner to one connection, or roll back without restarting.

The acquire/release SQL functions error out if a SQLite transaction is
currently open on the connection.

## API Reference

### Acquire Lock

```
POST /nslock/acquire?ns=<ns-id-hex>
Content-Type: application/json

{"owner": "client-identifier", "version": "optional-version-hex"}
```

| Status | Meaning |
|---|---|
| 201 | Lock acquired (or already owned by same owner) |
| 400 | Invalid owner (empty or > 256 bytes) |
| 409 | Locked by different owner (`x-lock-owner` header set) |

### Release Lock

```
POST /nslock/release?ns=<ns-id-hex>
Content-Type: application/json

{"owner": "client-identifier", "mode": "commit" | "rollback"}
```

| Status | Meaning |
|---|---|
| 200 | Rollback completed |
| 201 | Commit-mode release completed |
| 400 | Invalid owner |
| 410 | Lock lost during rollback (nonce mismatch) |
| 422 | Namespace not locked, or locked by different owner |

## Design Properties

- **Nonce-based safety**: A random 16-byte nonce prevents a stale rollback from corrupting data if the lock was released and re-acquired by another client between batches.
- **MVCC for concurrent readers**: Non-owners get a consistent snapshot at `snapshot_version`. Reads are never blocked.
- **Idempotent acquire**: Same owner calling acquire multiple times is safe.
- **Resumable rollback**: The cursor-based batch scan survives FDB transaction time limits and can resume after transient failures.
- **Separate from SQLite locking**: This operates at the distributed namespace level, independent of SQLite's page-level locking.

## Typical Usage Pattern

### Via HTTP

```
1. POST /nslock/acquire?ns=XX  {"owner": "worker-1"}
2. Run SQLite operations with MVSQLITE_LOCK_OWNER=worker-1
   (other writers are blocked; other readers see frozen snapshot)
3a. On success: POST /nslock/release?ns=XX  {"owner": "worker-1", "mode": "commit"}
3b. On failure: POST /nslock/release?ns=XX  {"owner": "worker-1", "mode": "rollback"}
   (all writes since acquire are erased)
```

### Via SQL

```sql
SELECT mv_nslock_acquire('main', 'worker-1');
-- ... do work; subsequent commits on this connection carry lock_owner=worker-1
SELECT mv_nslock_release_commit('main', 'worker-1');
-- or, to discard everything written since acquire:
SELECT mv_nslock_release_rollback('main', 'worker-1');
```
