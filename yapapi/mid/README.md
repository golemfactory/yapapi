# Yapapi refactoring

## Current state

1. Things described in WO\_1 should be ready.
2. Interface of WO\_1 objects is not super-polished, but there are few other things instead:
    * `Offer` - except for mocked negotiations, it is quite ready
    * `Agreement` - can be created/approved/terminated (only)
    * POC of the higher-level interface

## Examples

1. `example.py` - simple functions demonstrating selected parts of the interface
2. `t/test_1` - few simple tests
3. `cli_example.sh`
4. `run.py` - POC of the higher-level interface

## Code

General note: code will be almost certainly reordered (also: namespace `yapapi/mid` is not final), this is TBD.

Important parts of the code:

* yapapi/mid/golem\_node.py - GolemNode, the only entrypoint
* yapapi/mid/resource.py    - base class for all resources (defined as "any object in the Golem Network with an ID")
* yapapi/mid/market.py      - market API resources
* yapapi/mid/payment.py     - payment API resources
* yapapi/mid/cli            - cli (imported in `yapapi/__main__`)
* yapapi/mid/yagna\_event\_collector.py - Class that processes `yagna` events

Parts that will be important in the future, but now they are far-from-ready drafts:

* yapapi/mid/chain          - POC of the higher-level interface
* yapapi/mid/events.py      - new version of the current `yapapi/events.py`
* yapapi/mid/event\_bus.py  - interface for emitting/receiving events
* yapapi/mid/exceptions.py  - exceptions raised from the `yapapi/mid` code
* yapapi/mid/api\_call\_wrapper.py - a wrapper for all API calls

## CLI

```bash
python3 -m yapapi status

python3 -m yapapi find-node --runtime vm
python3 -m yapapi find-node --runtime vm --subnet public-beta 
python3 -m yapapi find-node --runtime vm --timeout 7  # stops after 7  seconds
python3 -m yapapi find-node --runtime vm --timeout 1m # stops after 60 seconds

python3 -m yapapi allocation list

python3 -m yapapi allocation new 1
python3 -m yapapi allocation new 2 --driver erc20 --network rinkeby

python3 -m yapapi allocation clean
```

"Status" command is not really useful now, but we don't yet have components to make it good.
