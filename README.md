# mvsqlite

Distributed, MVCC SQLite that runs on top of [FoundationDB](https://github.com/apple/foundationdb).

## Features

- **Time travel**: Immediately checkout past versions of your database.
- **Fork**: Create a writable snapshot from any past version of your database.
- **Zero-cost cross-database transactions**: While each SQLite database remains single-writer, you can horizontally scale your application with cross-database serializable transactions without additional overhead.
- **All the benefits from FoundationDB**: Correctness, fast distributed transactions, synchronous and asynchronous replication.
- **Minus FoundationDB's limits**: There's no [five-second limit](https://apple.github.io/foundationdb/known-limitations.html) on transactions any more, and a SQLite transaction can be 100x larger than a native one.

## Try it

TODO
