# Yapapi refactoring

## Current state

WO\_1 should be more-or-less ready.

## CLI

```bash
python3 -m yapapi status

python3 -m yapapi find-node --runtime vm
python3 -m yapapi find-node --runtime vm --subnet public-beta 
python3 -m yapapi find-node --runtime vm --timeout 7  # 7  seconds
python3 -m yapapi find-node --runtime vm --timeout 1m # 60 seconds

python3 -m yapapi allocation list

python3 -m yapapi allocation new 1
python3 -m yapapi allocation new 2 --driver erc20 --network rinkeby

python3 -m yapapi allocation clean
```

"Status" command is not really useful now, but we don't yet have components to make it good.
