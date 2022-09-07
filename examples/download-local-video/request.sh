#!/bin/sh

cd /golem/output
out=$(/usr/bin/time -o out.time -f "%e" wget -O /dev/null $1) && cat out.time && cat out.time > /golem/output/output.txt
