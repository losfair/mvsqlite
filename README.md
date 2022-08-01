# mvsqlite

Distributed, MVCC SQLite that runs on top of [FoundationDB](https://github.com/apple/foundationdb).

**This is alpha software and has not received enough testing. On-disk format may change in future versions. Please do not use it in production.**

- [mvsqlite](#mvsqlite)
  - [Features](#features)
    - [Upcoming features](#upcoming-features)
  - [Comparison with dqlite and rqlite](#comparison-with-dqlite-and-rqlite)
  - [Demo](#demo)
  - [Try it](#try-it)
  - [Caveats](#caveats)
    - [The "database is locked" error](#the-database-is-locked-error)
    - [No ABA-style idempotency](#no-aba-style-idempotency)
  - [Limits](#limits)
    - [Read latency](#read-latency)
    - [Not yet implemented: garbage collection](#not-yet-implemented-garbage-collection)

## Features

- **Full feature-set from SQLite**: mvsqlite integrates with SQLite using a custom [VFS](https://www.sqlite.org/vfs.html) layer.
- **Time travel**: Checkout the snapshot of your database at any point of time in the past.
- **Get the nice properties from FoundationDB, without its limits**: [Correctness](https://apple.github.io/foundationdb/testing.html), [really fast and scalable](https://apple.github.io/foundationdb/performance.html) distributed transactions, synchronous and asynchronous replication, integrated backup and restore. Meanwhile, there's no [five-second transaction limit](https://apple.github.io/foundationdb/known-limitations.html) any more, and a SQLite transaction can be 50x larger than FDB's native one.
- **Drop-in replacement**: Set the `LD_PRELOAD=libmvsqlite_preload.so` environment variable and your existing apps will work out of the box.

### Upcoming features

- **Branching**: Create a writable snapshot from a past version of your database.
- **Zero-overhead cross-database transactions**: While each SQLite database remains single-writer, you can horizontally scale your application with cross-database serializable transactions without additional overhead.

## Comparison with dqlite and rqlite

[dqlite](https://github.com/canonical/dqlite) and [rqlite](https://github.com/rqlite/rqlite) are two other distributed databases built on SQLite. Some of the key differences between mvsqlite and those two systems:

- **(+)**: mvsqlite is a drop-in replacement. To run existing applications with mvsqlite, setting `LD_PRELOAD` is enough.
- **(+)**: mvsqlite runs on a production-grade distributed key-value store, FoundationDB, instead of implementing its own consensus subsystem.
- **(+)**: mvsqlite has advanced multi-version features like snapshot reads without time limit, and reading the DB snapshot from a past point in time.
- **(+)**: mvsqlite supports multi-database transactions. So you can scale your writes horizontally, with the right data model.
- **(-)**: Reads in mvsqlite are sensitive to network latency. The client is stateless, and data needs to be fetched from FDB on demand.
  - Maybe this will be improved in a future version with a better prefetch strategy.
- **(-)**: mvsqlite is a little more complex to deploy than the alternative.
  - Three moving parts: FoundationDB, `mvstore`, and `libmvsqlite_preload.so`, instead of a single library.
- **(-)**: mvsqlite is new. Really new. And has not received as much testing as the alternatives.
  - But you can rely on FDB's continuous backup to ensure your data integrity in case an unknown bug in mvsqlite corrupted your database.

## Demo

**Time travel: checkout past snapshots**

Use the format `namespace@version` for the database name passed to SQLite:

![time travel](https://img.planet.ink/zhy/2022-07-27-154fef13e84d-207ea4945637b054b98be711396adc94.png)

## Try it

Install FoundationDB:

```bash
wget https://github.com/apple/foundationdb/releases/download/7.1.15/foundationdb-clients_7.1.15-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.1.15-1_amd64.deb
wget https://github.com/apple/foundationdb/releases/download/7.1.15/foundationdb-server_7.1.15-1_amd64.deb
sudo dpkg -i foundationdb-server_7.1.15-1_amd64.deb
```

Build and run `mvstore`, the server-side half that should be colocated with the FoundationDB cluster in production:

```bash
cargo build --release -p mvstore
RUST_LOG=info ./target/release/mvstore \
  --data-plane 127.0.0.1:7000 \
  --admin-api 127.0.0.1:7001 \
  --metadata-prefix mvstore-test \
  --raw-data-prefix m
```

Create a namespace with the admin API:

```bash
curl http://localhost:7001/api/create_namespace -i -d '{"key":"test","metadata":""}'
```

Build the client library:

```bash
cargo build --release -p mvsqlite
make -C ./mvsqlite-preload
```

Build `libsqlite3` and the `sqlite3` CLI: (note that a custom build is only needed here because the `sqlite3` binary shipped on most systems are statically linked to `libsqlite3` and `LD_PRELOAD` don't work)

```bash
wget https://www.sqlite.org/2022/sqlite-amalgamation-3390200.zip
unzip sqlite-amalgamation-3390200.zip
cd sqlite-amalgamation-3390200
gcc -O2 -fPIC --shared -o libsqlite3.so ./sqlite3.c -lpthread -ldl -lm
gcc -O2 -o sqlite3 ./shell.c -L. -lsqlite3
```

Set environment variables, and run the shell:

```bash
export RUST_LOG=info MVSQLITE_DATA_PLANE="http://localhost:7000"

# "test" is the key of the namespace we created earlier
LD_PRELOAD=../mvsqlite-preload/libmvsqlite_preload.so LD_LIBRARY_PATH=. ./sqlite3 test
```

You should see the sqlite shell now :) Try creating a table and play with it.

## Caveats

This section documents various behaviors of mvsqlite that are a little different from SQLite. These differences **do not cause correctness issues**, but may confuse some applications a bit.

### The "database is locked" error

To keep compatibility with applications targeting upstream SQLite, mvsqlite enables pessimistic locking by default - when a transaction takes a RESERVED or higher lock on a database, it acquires a one-minute lock lease from mvstore to prevent another transaction from writing to the database. At this point, we have the chance to fail gracefully and return a "database is locked" error if multiple clients want to acquire lock on the same DB. This is a best-effort mechanism to prevent conflict on commit (which causes the process to abort).

Apps should always set `busy_timeout = 0` (which is the default) when running with mvsqlite, because on a "database is locked" error it is not possible to retry later and succeed with the same transaction.

### No ABA-style idempotency

In case of network errors and crashes, mvsqlite implements AA-style idempotency. *Continuously* retrying the same commit will keep returning the same successful result. But in case the global commit order is A-B-A, the second attempt of the A commit will conflict and fail.

This means that, in a very rare circumstance as described below:

> mvstore crashed during commit, just between FDB commit success and returning the result to the client. Then, another client acquired the database lock, wrote to the database, and committed successfully. Now, the first client retries the commit.

The first client will get a commit conflict and abort. This is the expected behavior, since unbounded idempotency requires too much overhead.

## Limits

### Read latency

SQLite does synchronous "disk" I/O. While we can (and do) concurrently execute write operations, reads from FoundationDB block the SQLite thread.

This is probably fine if you don't expect to get very I/O intensive on a single database, but you may want to enable `coroutine` in the `IoEngine` config if you have an event loop outside, so that network I/O won't block the thread.

### Not yet implemented: garbage collection

Currently history versions will be kept in the database forever. There is no garbage collection yet. In a future version this will be fixed.
