# mvSQLite

Distributed, MVCC SQLite that runs on top of [FoundationDB](https://github.com/apple/foundationdb).

[Documentation](https://github.com/losfair/mvsqlite/wiki/)

**mvSQLite needs your help**: I consider mvSQLite to be *early-stage beta* software. Known high-severity bugs are fixed, I've been running this for a while without problems, but there are not enough tests under more diverse contexts. Testing under different use cases is appreciated, and please open an issue or contact me with the email in my profile if you want to file bug reports or have any kind of suggestions!

- [mvSQLite](#mvsqlite)
  - [Features](#features)
  - [Releases](#releases)
  - [Demo](#demo)
  - [Try it](#try-it)
  - [Contributing](#contributing)

## Features

- **Full feature-set from SQLite**: mvsqlite integrates with SQLite using either a custom [VFS](https://www.sqlite.org/vfs.html) layer or [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) at your choice. Since it is a layer "below" SQLite itself, all of SQLite's features are available.
- **Time travel**: Checkout the snapshot of your database at any point of time in the past.
- **Scalable reads and writes**: Optimistic, fine-grained concurrency with [BEGIN CONCURRENT](https://www.sqlite.org/cgi/src/doc/begin-concurrent/doc/begin_concurrent.md)-like semantics. See [this page](https://github.com/losfair/mvsqlite/wiki/Concurrency-and-conflict-check) for details.
- **Get the nice properties from FoundationDB, without its limits**: [Correctness](https://apple.github.io/foundationdb/testing.html), [really fast and scalable](https://apple.github.io/foundationdb/performance.html) distributed transactions, synchronous and asynchronous replication, integrated backup and restore. Meanwhile, there's no [five-second transaction limit](https://apple.github.io/foundationdb/known-limitations.html) any more, and a SQLite transaction can be ~39x larger than FDB's native transaction.
- **Drop-in addition**: Use either `LD_PRELOAD` or FUSE to plug mvSQLite into your existing apps. [Read the docs](https://github.com/losfair/mvsqlite/wiki/Integration)

## Releases

Grab the latest binaries from the [Releases](https://github.com/losfair/mvsqlite/releases) page. You can also [build your own binaries](#contributing) to run on a platform other than x86-64.

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

Download the binaries:

```bash
curl -L -o ./libmvsqlite_preload.so https://github.com/losfair/mvsqlite/releases/download/v0.1.16/libmvsqlite_preload.so
curl -L -o ./mvstore https://github.com/losfair/mvsqlite/releases/download/v0.1.16/mvstore
chmod +x ./mvstore
```

Run `mvstore`, the server-side half that should be colocated with the FoundationDB cluster in production:

```bash
RUST_LOG=info ./mvstore \
  --data-plane 127.0.0.1:7000 \
  --admin-api 127.0.0.1:7001 \
  --metadata-prefix mvstore-test \
  --raw-data-prefix m
```

Create a namespace with the admin API:

```bash
curl http://localhost:7001/api/create_namespace -i -d '{"key":"test","metadata":""}'
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
