#!/bin/sh

out=$(curl -X 'GET' 'https://google.com') && echo "$out" > /golem/output/output.txt
