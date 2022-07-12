#!/usr/bin/env bash

printf "*** STATUS ***\n"
python3 -m yapapi status

printf "\n*** FIND-NODE ***\n"
python3 -m yapapi find-node --runtime vm --timeout 1

printf "\n*** ALLOCATION LIST ***\n"
python3 -m yapapi allocation list

printf "\n*** ALLOCATION NEW ***\n"
python3 -m yapapi allocation new 50 --network polygon

*   q_reqc TODOs
printf "\n*** ALLOCATION CLEAN ***\n"
python3 -m yapapi allocation clean
