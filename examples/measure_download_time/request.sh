#!/bin/sh

/usr/bin/time -o output.txt -f "%e" wget -O /dev/null $1 && cat output.txt > /golem/output/output.txt
