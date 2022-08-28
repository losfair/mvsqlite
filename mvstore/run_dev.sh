#!/bin/bash

set -e

#export RUST_LOG=info,mvstore=debug
export RUST_LOG=info
./target/release/mvstore --data-plane 127.0.0.1:7000 --admin-api 127.0.0.1:7001 --metadata-prefix mvstore-test --raw-data-prefix m \
  --content-cache ./work/content --content-cache-size 2000000000
