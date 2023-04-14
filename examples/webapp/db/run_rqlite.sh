#!/bin/bash
/bin/rqlited -http-addr 0.0.0.0:4001 /rqlite/file/data > /tmp/rqlited_out 2> /tmp/rqlited_err &
