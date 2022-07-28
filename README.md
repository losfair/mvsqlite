# mvsqlite

Distributed, MVCC SQLite that runs on top of [FoundationDB](https://github.com/apple/foundationdb).

**This is alpha software and has not received enough testing. On-disk format may change in future versions. Please do not use it in production.**

- [mvsqlite](#mvsqlite)
  - [Features](#features)
    - [Upcoming features](#upcoming-features)
  - [Demo](#demo)
  - [Try it](#try-it)
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

## Demo

**Time travel: checkout past snapshots**

Use the format `namespace@version` for the database name passed to SQLite:

![time travel](https://img.planet.ink/zhy/2022-07-27-154fef13e84d-207ea4945637b054b98be711396adc94.png)

**Optimistic MVCC transactions**

SQLite is a single-writer database - this isn't going to change easily, due to its fundamental design choices.

But a group of N sqlite databases is an N-writer database. And mvsqlite provides the necessary mechanisms to do serializable cross-database transactions without additional overhead.

The logic for cross-database transactions isn't there yet, so here's a demo that shows how MVCC works (and how it can break things) in mvsqlite when there are more than one concurrent writers to the same database.

![mvcc](https://img.planet.ink/zhy/2022-07-27-154f742d16d0-5bb18e5c83df84a29f898f02067fbdb2.png)

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
```

Patch and build the `sqlite3` CLI binary with mvsqlite patch:

```bash
wget https://www.sqlite.org/2022/sqlite-amalgamation-3390200.zip
unzip sqlite-amalgamation-3390200.zip
cd sqlite-amalgamation-3390200
patch < ../misc/sqlite-amalgamation-3390200-shell.patch
gcc -O2 -o sqlite3 ./*.c -L../target/release -lmvsqlite -lssl -lcrypto -lpthread -ldl -lm
```

Set environment variables, and run the shell:

```bash
export RUST_LOG=info MVSQLITE_DATA_PLANE="http://localhost:7000"
./sqlite3 test # "test" is the key of the namespace we created earlier
```

You should see the sqlite shell now :)

## Limits

### Read latency

SQLite does synchronous "disk" I/O. While we can (and do) concurrently execute write operations, reads from FoundationDB block the SQLite thread.

This is probably fine if you don't expect to get very I/O intensive on a single database, but you may want to enable `coroutine` in the `IoEngine` config so that network I/O won't block the thread if you have an event loop outside.

### Not yet implemented: garbage collection

Currently history versions will be kept in the database forever. There is no garbage collection yet. In a future version this will be fixed.
