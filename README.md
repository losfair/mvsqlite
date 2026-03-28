# mvSQLite

Distributed, MVCC SQLite that runs on top of [FoundationDB](https://github.com/apple/foundationdb).

[Documentation](https://github.com/losfair/mvsqlite/wiki/)

- [mvSQLite](#mvsqlite)
    - [Features](#features)
    - [Experimental Features](#experimental-features)
    - [Releases](#releases)
    - [Quick reference](#quick-reference)
    - [Try it](#try-it)
    - [Contributing](#contributing)

## Features

- **Full feature-set from SQLite**: mvSQLite integrates with SQLite as a custom [VFS](https://www.sqlite.org/vfs.html) layer. It is a layer "below" SQLite, and all of SQLite's features are available.
- **Time travel**: Checkout the snapshot of your database at any point of time in the past.
- **Lock-free, scalable reads and writes**: Optimistic fine-grained concurrency with [BEGIN CONCURRENT](https://www.sqlite.org/cgi/src/doc/begin-concurrent/doc/begin_concurrent.md)-like semantics. mvSQLite inherits FoundationDB's lock-free property - not a single distributed lock is acquired during data plane operation.
- **Get the nice properties from FoundationDB, without its limits**: [Correctness](https://apple.github.io/foundationdb/testing.html), [really fast and scalable](https://apple.github.io/foundationdb/performance.html) distributed transactions, synchronous and asynchronous replication, integrated backup and restore. Meanwhile, there's no [five-second transaction limit](https://apple.github.io/foundationdb/known-limitations.html) any more, and a SQLite transaction can be ~39x larger than FDB's native transaction.
- **Drop-in addition**: Use `LD_PRELOAD` or a patched `libsqlite3.so` to plug mvSQLite into your existing apps. [Read the docs](https://github.com/losfair/mvsqlite/wiki/Integration)

## Experimental Features

These features are not considered stable yet and should not be used in production without validation.

### Commit Groups

Commit groups let you batch writes from multiple mvSQLite databases into a single FoundationDB commit.

Enable the SQL functions with:

```bash
export MVSQLITE_EXPERIMENTAL_COMMIT_GROUP=1
```

Available SQL functions:

- `mv_commit_group_begin()`
- `mv_commit_group_commit(db_name)`
- `mv_commit_group_rollback(db_name)`

Example:

```sql
ATTACH 'orders' AS orders;

SELECT mv_commit_group_begin();

BEGIN;
INSERT INTO main.accounts(id, balance) VALUES (1, 100);
COMMIT;

BEGIN;
INSERT INTO orders.invoices(id, account_id) VALUES (1, 1);
COMMIT;

SELECT mv_commit_group_commit('main');
```

Notes:

- `mv_commit_group_commit(db_name)` and `mv_commit_group_rollback(db_name)` require an explicit SQLite database name such as `'main'` or an attached database name.
- A commit-group conflict is returned as a SQLite `SQLITE_PERM` error.
- After the first grouped commit is staged, no new transaction may begin in the same group.

## Releases

Grab the latest binaries from the [Releases](https://github.com/losfair/mvsqlite/releases) page. You can also [build your own binaries](#contributing) to run on a platform other than x86-64.

## Quick reference

Check the single-page [mvSQLite Quick Reference](https://blob.univalent.net/mvsqlite-quick-reference-v0-2.pdf) for common operations with mvSQLite.

## Try it

Install FoundationDB:

```bash
wget https://github.com/apple/foundationdb/releases/download/7.3.69/foundationdb-clients_7.3.69-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.3.69-1_amd64.deb
wget https://github.com/apple/foundationdb/releases/download/7.3.69/foundationdb-server_7.3.69-1_amd64.deb
sudo dpkg -i foundationdb-server_7.3.69-1_amd64.deb
```

Download the binaries:

```bash
curl -L -o ./libmvsqlite_preload.so https://github.com/losfair/mvsqlite/releases/download/v0.2.1/libmvsqlite_preload.so
curl -L -o ./mvstore https://github.com/losfair/mvsqlite/releases/download/v0.2.1/mvstore
chmod +x ./mvstore
```

Run `mvstore`, the server-side half that should be colocated with the FoundationDB cluster in production:

```bash
RUST_LOG=info ./mvstore \
  --data-plane 127.0.0.1:7000 \
  --admin-api 127.0.0.1:7001 \
  --metadata-prefix mvstore \
  --raw-data-prefix m
```

Create a namespace with the admin API:

```bash
curl http://localhost:7001/api/create_namespace -i -d '{"key":"test"}'
```

Build `libsqlite3` and the `sqlite3` CLI: (note that a custom build is only needed here because the `sqlite3` binary shipped on most systems are statically linked to `libsqlite3` and `LD_PRELOAD` don't work)

```bash
wget https://sqlite.org/2026/sqlite-amalgamation-3510300.zip
unzip sqlite-amalgamation-3510300.zip
cd sqlite-amalgamation-3510300
gcc -O2 -fPIC --shared -o libsqlite3.so ./sqlite3.c -lpthread -ldl -lm
gcc -O2 -o sqlite3 ./shell.c -L. -lsqlite3
```

Set environment variables, and run the shell:

```bash
export RUST_LOG=info MVSQLITE_DATA_PLANE="http://localhost:7000"

# "test" is the key of the namespace we created earlier
LD_PRELOAD=../libmvsqlite_preload.so LD_LIBRARY_PATH=. ./sqlite3 test
```

You should see the sqlite shell now :) Try creating a table and play with it.

## Contributing

mvsqlite can be built with the standard Rust toolchain:

```bash
cargo build --release -p mvstore
cargo build --release -p mvsqlite
make -C mvsqlite-preload
```

Internals are documented in the [wiki](https://github.com/losfair/mvsqlite/wiki).
