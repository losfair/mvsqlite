#!/bin/bash

set -e

gcc -O2 -o sqlite3 ./*.o -L/home/ubuntu/Projects/mvsqlite/target/debug -lmvsqlite -lssl -lcrypto -lpthread -ldl -lm
