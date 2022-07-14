#!/usr/bin/env bash

printf "*** STATUS ***\n"
python3 -m yapapi status

printf "\n*** FIND-NODE ***\n"
python3 -m yapapi find-node --runtime vm --timeout 1

printf "\n*** ALLOCATION LIST ***\n"
python3 -m yapapi allocation list

printf "\n*** ALLOCATION NEW ***\n"
python3 -m yapapi allocation new 1

printf "\n*** ALLOCATION NEW ***\n"
python3 -m yapapi allocation new 2 --driver erc20 --network rinkeby

printf "\n*** ALLOCATION LIST ***\n"
python3 -m yapapi allocation list

printf "\n*** ALLOCATION CLEAN ***\n"
python3 -m yapapi allocation clean

printf "\n*** ALLOCATION LIST ***\n"
python3 -m yapapi allocation list
