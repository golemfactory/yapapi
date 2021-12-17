# Golem Python API

[![Tests - Status](https://img.shields.io/github/workflow/status/golemfactory/yapapi/Continuous%20integration/master?label=tests)](https://github.com/golemfactory/yapapi/actions?query=workflow%3A%22Continuous+integration%22+branch%3Amaster)
![PyPI - Status](https://img.shields.io/pypi/status/yapapi)
[![PyPI version](https://badge.fury.io/py/yapapi.svg)](https://badge.fury.io/py/yapapi)
[![GitHub license](https://img.shields.io/github/license/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/issues)

## Installation

`yapapi` is available as a [PyPI package](https://pypi.org/project/yapapi/0.6.2/).

You can install it through `pip`:
```
pip install yapapi
```

Or if your project uses [`poetry`](https://python-poetry.org/) you can add it to your dependencies like this:
```
poetry add yapapi
```

### Local development setup

#### Poetry
`yapapi` uses [`poetry`](https://python-poetry.org/) to manage its dependencies and provide a runner for common tasks.

If you don't have `poetry` available on your system then follow its [installation instructions](https://python-poetry.org/docs/#installation) before proceeding.
Verify your installation by running:
```
poetry --version
```

#### Project dependencies
To install the project's dependencies run:
```
poetry install
```
By default, `poetry` looks for the required Python version on your `PATH` and creates a virtual environment for the project if there's none active (or already configured by Poetry).

All of the project's dependencies will be installed to that virtual environment.

If you'd like to run the `yapapi` integration test suite locally then you'll need to install an additional set of dependencies separately by running:
```
poetry install -E integration-tests
```
## What's Golem, btw?

[Golem](https://golem.network) is a global, open-source, decentralized supercomputer
that anyone can access. It connects individual machines - be that laptops, home PCs or
even data centers - to form a vast network, the purpose of which is to provide a way to
distribute computations to its provider nodes and allow requestors to utilize its unique
potential - which can lie in its combined computing power, the geographical distribution
or its censorship resistance.

## Golem's requestor setup

Golem's requestor-side configuration consists of two separate components:
* the [`yagna` daemon](https://github.com/golemfactory/yagna) - your node in the
  new Golem network, responsible for communication with the other nodes, running the
  market and providing easy access to the payment mechanisms.
* the requestor agent - the part that the developer of the specific Golem application
  is responsible for.

The daemon and the requestor agent communicate using three REST APIs which
`yapapi` - Golem's Python high-level API - aims to abstract to large extent to make
application development on Golem as easy as possible.

## How to use this API?

Assuming you have your Golem node up and running (you can find instructions on how to
do that in the [yagna repository](https://github.com/golemfactory/yagna) and in our
[handbook](https://handbook.golem.network)), what you need to do is:
* **prepare your payload** - this needs to be a Docker image containing your application
  that will be executed on the provider's end. This image needs to have its volumes
  mapped in a way that will allow the supervisor module to exchange data (write and
  read files) with it. This image needs to be packed and uploaded into Golem's image repository
  using our dedicated tool - [`gvmkit-build`](https://pypi.org/project/gvmkit-build/).
* **create your requestor agent** - this is where `yapapi` comes in. Utilizing our high-level
  API, the creation of a requestor agent should be straightforward and require minimal effort.
  You can use examples contained in this repository as references, the directory
  [`examples/hello-world/`](https://github.com/golemfactory/yapapi/tree/master/examples/hello-world)
  contains minimal examples of fully functional requestor agents and
  is therefore the best place to start exploring.

### Components

There are a few components that are crucial for any requestor agent app:

#### Golem

The heart of the high-level API is the `Golem` class (`yapapi.Golem`), which serves as 
the "engine" of a requestor agent. `Golem` is responsible for finding providers interested in
the jobs you want to execute, negotiating agreements with them and processing payments. It also
implements core functionality required to execute commands on providers that have signed such agreements.

`Golem` provides two entry points for executing jobs on the Golem network, corresponding to the
two basic modes of operation of a requestor agent:

* The method `execute_tasks` allows you to submit a _task-based_ job for execution. Arguments
to this method must include a sequence of independent _tasks_ (units of work) to be distributed
among providers, a _payload_ (a VM image) required to compute them, and a _worker_ function, which
will be used to convert each task to a sequence of steps to be executed on a provider. You may also
specify the timeout for the whole job, the maximum number of providers used at any given time,
and the maximum amount that you want to spend.

* The method `run_service` allows you, as you probably guessed, to run a _service_ on Golem. 
Instead of a task-processing worker function, an argument to `run_service` is a
class (a subclass of `yapapi.Service`) that implements the behaviour of your service in various
stages of its lifecycle (when it's _starting_, _running_ etc.). Additionally, you may specify
the number of _service instances_ you want to run and the service expiration datetime.

Prior to version `0.6.0`, only task-based jobs could be executed. For more information on both types of jobs please refer to our [handbook](https://handbook.golem.network).
 
#### Worker function

The worker will most likely be the very core of your task-based requestor app.
You need to define this function in your agent code and then you pass it (as the value of
the `worker` parameter) to the `execute_tasks` method of `Golem`.

The worker receives a _work context_ (`yapapi.WorkContext`) object that serves
as an interface between your script and the execution unit within the provider.
Using the work context, you define the steps that the provider needs to execute in order
to complete the job you're giving them - e.g. transferring files to and from the provider
or running commands within the execution unit on the provider's end.

Depending on the number of workers, and thus, the maximum number of providers that 
`execute_tasks` utilizes in parallel, a single worker may tackle several tasks
and you can differentiate the steps that need to happen once
per worker run, which usually means once per provider node - but that depends on the
exact implementation of your worker function - from those that happen for each
task. An example of the former would be an upload of a source
file that's common to each task; and of the latter - a step that triggers the
processing of the file using a set of parameters specified for a particular task.

#### Task

The _task_ (`yapapi.Task`) object describes a unit of work that your application needs
to carry out.

`Golem` will feed an instance of your worker - bound to a single provider node -
with `Task` objects. The worker will be responsible for completing those tasks. Typically,
it will turn each task into a sequence of steps to be executed in a single run
of the execution script on a provider's machine, in order to compute the task's result.


### Example

An example task-based Golem application, using a minimal Docker image
(Python file with the example and the Dockerfile for the image reside in
[`examples/hello-world/`](https://github.com/golemfactory/yapapi/tree/master/examples/hello-world)):
```python
import asyncio
from typing import AsyncIterable

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        script = context.new_script()
        future_result = script.run("/bin/sh", "-c", "date")

        yield script
        task.accept_result(result=await future_result)


async def main():
    package = await vm.repo(
        image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
    )

    tasks = [Task(data=None)]

    async with Golem(budget=1.0, subnet_tag="devnet-beta") as golem:
        async for completed in golem.execute_tasks(worker, tasks, payload=package):
            print(completed.result.stdout)


if __name__ == "__main__":
    enable_default_logger(log_file="hello.log")

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
```

## Environment variables

It's possible to set various elements of `yagna` configuration through environment variables.
`yapapi` currently supports the following environment variables:
- `YAGNA_ACTIVITY_URL`, URL to `yagna` activity API, e.g. `http://localhost:7500/activity-api/v1`
- `YAGNA_API_URL`, base URL to `yagna` REST API, e.g. `http://localhost:7500`
- `YAGNA_APPKEY`, `yagna` app key to be used, e.g. `a70facb9501d4528a77f25574ab0f12b`
- `YAGNA_MARKET_URL`, URL to `yagna` market API, e.g. `http://localhost:7500/market-api/v1`
- `YAGNA_PAYMENT_NETWORK`, Ethereum network name for `yagna` to use, e.g. `rinkeby`
- `YAGNA_PAYMENT_DRIVER`, payment driver name for `yagna` to use, e.g. `erc20`
- `YAGNA_PAYMENT_URL`, URL to `yagna` payment API, e.g. `http://localhost:7500/payment-api/v1`
- `YAGNA_SUBNET`, name of the `yagna` sub network to be used, e.g. `devnet-beta`
- `YAPAPI_USE_GFTP_CLOSE`, if set to a _truthy_ value (e.g. "1", "Y", "True", "on") then `yapapi`
  will ask `gftp` to close files when there's no need to publish them any longer. This may greatly
  reduce the number of files kept open while `yapapi` is running but requires `yagna`
  0.7.3 or newer, with older versions it will cause errors.
