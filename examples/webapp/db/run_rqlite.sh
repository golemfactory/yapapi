#!/bin/bash
/bin/rqlited -http-addr 0.0.0.0:4001 /rqlite/file/data > out.log 2> err.log &
