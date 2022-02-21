#!/bin/bash
/bin/rqlited -http-addr 0.0.0.0:4001 /rqlite/file/data > /run/out 2> /run/err &
