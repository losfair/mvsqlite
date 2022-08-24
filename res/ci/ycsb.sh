#!/bin/bash

set -e

DBNAME="$1"
WORKLOAD="$2"

if [ ! -d "go-ycsb-src" ]; then
  git clone https://github.com/losfair/go-ycsb go-ycsb-src
fi

cd go-ycsb-src
git checkout e6bad4a1af10fbd4d921c5f578d7d57cf80b08ea

LD_PRELOAD="$PRELOAD_PATH" ../go-ycsb/go-ycsb load sqlite \
  -P workloads/$WORKLOAD \
  -p operationcount=100000 -p threadcount=1 -p recordcount=100000 \
  -p sqlite.db=$DBNAME \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.cache=private \
  -p sqlite.optimistic=true \
  -p batch.size=1000 | tee load.log

grep -vzq "_ERROR" load.log

RUN_SIZE=100000
if [ "$WORKLOAD" == "workloada" ]; then
  RUN_SIZE=10000 # workloada is slow
fi

LD_PRELOAD="$PRELOAD_PATH" ../go-ycsb/go-ycsb run sqlite \
  -P workloads/$WORKLOAD \
  -p operationcount=$RUN_SIZE -p threadcount=64 -p recordcount=100000 \
  -p sqlite.db=$DBNAME \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.optimistic=true \
  -p sqlite.cache=private | tee run.log

grep -vzq "_ERROR" run.log
