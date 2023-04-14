#!/bin/bash
/bin/rqlited -http-addr 0.0.0.0:4001 /rqlite/file/data > /root/.local/share/ya-provider/exe-unit/work/out.log 2> /root/.local/share/ya-provider/exe-unit/work/err.log &
