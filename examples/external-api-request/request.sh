#!/bin/sh

out=$(curl -X 'GET' 'https://httpbin.org/get') && echo "$out" > /golem/output/output.txt
