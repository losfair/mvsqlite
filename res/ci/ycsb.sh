#!/bin/bash

set -e

LD_PRELOAD="$PRELOAD_PATH" ./go-ycsb/go-ycsb load sqlite \
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

RUN_SIZE=100000
if [ "$1" == "workloada" ]; then
  RUN_SIZE=10000 # workloada is slow
fi

LD_PRELOAD="$PRELOAD_PATH" ./go-ycsb/go-ycsb run sqlite \
  -P workloads/$1 \
  -p operationcount=$RUN_SIZE -p threadcount=64 -p recordcount=100000 \
  -p sqlite.db=ycsb \
  -p sqlite.journalmode=delete \
  -p sqlite.maxopenconns=1000 \
  -p sqlite.maxidleconns=1000 \
  -p sqlite.optimistic=true \
  -p sqlite.cache=private | tee run.log

grep -vzq "_ERROR" run.log
