#!/bin/bash

set -e

export MVSQLITE_PAGE_CACHE_SIZE=50000
export MVSQLITE_DATA_PLANE=http://localhost:7000
export RUST_LOG=off

git clone https://github.com/losfair/go-ycsb
cd go-ycsb
git checkout e6bad4a1af10fbd4d921c5f578d7d57cf80b08ea
make

LD_PRELOAD="$PRELOAD_PATH" ./bin/go-ycsb load sqlite \
  -P workloads/$1 \
  -p operationcount=100000 -p threadcount=1 -p recordcount=100000 \
  -p sqlite.db=ycsb \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.cache=private \
  -p sqlite.optimistic=true \
  -p batch.size=1000 | tee load.log

grep -vzq "_ERROR" load.log

LD_PRELOAD="$PRELOAD_PATH" ./bin/go-ycsb run sqlite \
  -P workloads/$1 \
  -p operationcount=100000 -p threadcount=64 -p recordcount=100000 \
  -p sqlite.db=ycsb \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.optimistic=true \
  -p sqlite.cache=private | tee run.log

grep -vzq "_ERROR" run.log
