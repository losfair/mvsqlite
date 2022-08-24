#!/bin/bash

set -e

export MVSQLITE_PAGE_CACHE_SIZE=50000
export MVSQLITE_DATA_PLANE=http://localhost:7000
export RUST_LOG=off

git clone https://github.com/losfair/go-ycsb
cd go-ycsb
git checkout 3e38695ae2a7cd318600faaa79c4191e7ed1efbf
make

LD_PRELOAD="$PRELOAD_PATH" ./bin/go-ycsb load sqlite \
  -P workloads/workloadb \
  -p operationcount=100000 -p threadcount=1 -p recordcount=100000 \
  -p sqlite.db=ycsb \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.cache=private \
  -p batch.size=1000

LD_PRELOAD="$PRELOAD_PATH" ./bin/go-ycsb run sqlite \
  -P workloads/workloadb \
  -p operationcount=100000 -p threadcount=64 -p recordcount=100000 \
  -p sqlite.db=ycsb \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.cache=private
