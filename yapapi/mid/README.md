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
For me, this looks more like a new, separate library than refreshed `yapapi`. This should be discussed before WO\_2.

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
* yapapi/mid/default\_logger.py - listens for events, prints them to a file

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

## NEXT STEPS

How I think we should continue with this project.

### 1. Implement necessary low-level objects and create a first "working" app

Current idea for the first app: capabilities similar to the current Task API.
Purpose:

* Ensure the low-level interface is good enough to write this sort of mid-level API
  in a convenient way
* Have some benchmark - e.g. it should not be slower then the current Task API
* It's good to have some E2E piece of code as early as possible

Details:

* Code should be universal - implementation of the next apps should require changing only app-specific code.
* "Happy path" is enough - we don't have to handle unusual/unexpected errors ...
* ... but we should know how they should be handled in the future

Rough estimate: 10 MD

### 2. Implement few more apps, as diverse as possible

Purpose: 
* Writing/modifying apps with yapapi should be easy & fun. Here we try to find things that are not easy/fun
  and make them easier/more fun.
* We should try different "hard" things and ensure they are not that hard.
  + E.g. activity/agreement recycyling

App ideas:

* Service API compatibility POC 
  + We can run a service using the current Service class
  + We accept some things might not work
  + We don't have the Cluster/Network interface etc
  + (Bonus option: add a switch that turns off the "work generator" pattern)
* Verification-by-redundance POC
  + We have something like Task API, but with tasks repeated on different providers,
    task results from different providers are compared
* Collatz conjecture
  + Use as many activities as possible, with a simple cost limit (e.g. max estimated cost per hour)
* An app that requires two different demands
  + Purpose: ensure this is possible without much efforts
  + Could be something simple/stupid, no specific idea now

Details:

* App quality should match the quality of the first app (--> provious step)
* App code should be readable - without ugly hacks or weird patterns - developer should be able to understand
  how the app works and how to modify it. This is important.

Rough estimate: 2-3 MD per app.

### 3. Smoothen the edges. Implement "known" missing parts.

Details:

* Support for "expected" unhappy paths
* Missing parts of the negotiations
* Missing parts of the interface
* Network (just use the current `yapapi.network`? But this should be Resource?)
* ... (details after step 2)
* Maybe better docs?

Purpose: 

* Make it ready for internal tests. Ask Blue/Sieciech/Philip/etc to try it:
  + Best scenario: they conceive an app that is hard to write in this framework, and we know what needs changes
* Gather some feedback

Rough estimate: 3-10 MD

### 4. ...?

Business decision needed.

I see few different reasonable directions:

1. Public Alpha(Beta?)-quality release
2. More improvements, edge-smothing, going towards a "high quality" release
3. Don't push for the release, but start using internally. 
4. Create a first complete mainnet-production-grade app (cost management, reliability etc)
5. Provide full compatibility with the "old" high-level API
