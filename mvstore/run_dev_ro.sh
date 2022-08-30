#!/bin/bash

set -e

#export RUST_LOG=info,mvstore=debug
export RUST_LOG=info
./target/release/mvstore --data-plane 127.0.0.1:7100 --admin-api 127.0.0.1:7101 --metadata-prefix mvstore-test --raw-data-prefix m \
    --read-only --cluster /etc/foundationdb/fdb2.cluster
