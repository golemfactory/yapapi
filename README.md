# Golem Python API

[![Tests - Status](https://img.shields.io/github/workflow/status/golemfactory/yapapi/Continuous%20integration/master?label=tests)](https://github.com/golemfactory/yapapi/actions?query=workflow%3A%22Continuous+integration%22+branch%3Amaster)
![PyPI - Status](https://img.shields.io/pypi/status/yapapi)
[![PyPI version](https://badge.fury.io/py/yapapi.svg)](https://badge.fury.io/py/yapapi)
[![GitHub license](https://img.shields.io/github/license/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/issues)

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
  API, the creation of a requestor agent should be straighforward and require minimal effort.
  You can use examples contained in this repository (blender and hashcat) as references.

### Components

There are a few components that are crucial for any requestor agent app:

#### Executor

The heart of the high-level API is the requestor's task executor (`yapapi.Executor`).
You tell it, among others, which package (VM image) will be used to run your task,
how much you'd like to pay and how many providers you'd like to involve in the execution.
Finally, you feed it the worker script and a list of `Task` objects to execute on providers.

#### Worker script

The `worker` will most likely be the very core of your requestor app. You need to define
this function in your agent code and then you pass it to the Executor.

It receives a `WorkContext` (`yapapi.WorkContext`) object that serves
as an interface between your script and the execution unit within the provider.
Using the work context, you define the steps that the provider needs to execute in order
to complete the job you're giving them - e.g. transferring files to and from the provider
or running commands within the execution unit on the provider's end.

Depending on the number of workers, and thus, the maximum number of providers that your
Executor utilizes in parallel, a single worker may tackle several tasks
(units of your work) and you can differentiate the steps that need to happen once
per worker run, which usually means once per provider node - but that depends on the
exact implementation of your worker function - from those that happen for each
individual unit of work. An example of the former would be an upload of a source
file that's common to each fragment; and of the latter - a step that triggers the
processing of the file using a set of parameters specified in the `Task` data.

#### Task

The `Task` (`yapapi.Task`) object describes a unit of work that your application needs
to carry out.

The Executor will feed an instance of your worker - bound to a single provider node -
with `Task` objects. The worker will be responsible for completing those tasks. Typically,
it will turn each task into a sequence of steps to be executed in a single run
of the execution script on a provider's machine, in order to compute the task's result.


### Example

An example Golem application, using a Docker image containing the Blender renderer:

```python
import asyncio
from datetime import datetime, timedelta
import pathlib
import sys

from yapapi import (
    Executor,
    NoPaymentAccountError,
    Task,
    __version__ as yapapi_version,
    WorkContext,
    windows_event_loop_fix,
)
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.package import vm
from yapapi.rest.activity import BatchTimeoutError


async def main(subnet_tag, driver=None, network=None):
    package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):
        script_dir = pathlib.Path(__file__).resolve().parent
        scene_path = str(script_dir / "cubes.blend")
        ctx.send_file(scene_path, "/golem/resource/scene.blend")
        async for task in tasks:
            frame = task.data
            crops = [{"outfilebasename": "out", "borders_x": [0.0, 1.0], "borders_y": [0.0, 1.0]}]
            ctx.send_json(
                "/golem/work/params.json",
                {
                    "scene_file": "/golem/resource/scene.blend",
                    "resolution": (400, 300),
                    "use_compositing": False,
                    "crops": crops,
                    "samples": 100,
                    "frames": [frame],
                    "output_format": "PNG",
                    "RESOURCES_DIR": "/golem/resources",
                    "WORK_DIR": "/golem/work",
                    "OUTPUT_DIR": "/golem/output",
                },
            )
            ctx.run("/golem/entrypoints/run-blender.sh")
            output_file = f"output_{frame}.png"
            ctx.download_file(f"/golem/output/out{frame:04d}.png", output_file)
            try:
                # Set timeout for executing the script on the provider. Two minutes is plenty
                # of time for computing a single frame, for other tasks it may be not enough.
                # If the timeout is exceeded, this worker instance will be shut down and all
                # remaining tasks, including the current one, will be computed by other providers.
                yield ctx.commit(timeout=timedelta(seconds=120))
                # TODO: Check if job results are valid
                # and reject by: task.reject_task(reason = 'invalid file')
                task.accept_result(result=output_file)
            except BatchTimeoutError:
                print(
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                )
                raise

    frames: range = range(0, 60, 10)
    init_overhead = 3
    min_timeout, max_timeout = 6, 30

    timeout = timedelta(minutes=max(min(init_overhead + len(frames) * 2, max_timeout), min_timeout))

    async with Executor(
        package=package,
        max_workers=3,
        budget=10.0,
        timeout=timeout,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
        event_consumer=log_summary(),
    ) as executor:

        sys.stderr.write(
            f"yapapi version: {yapapi_version}\n"
            f"Using subnet: {subnet_tag}, "
            f"payment driver: {executor.driver}, "
            f"and network: {executor.network}\n"
        )

        num_tasks = 0
        start_time = datetime.now()

        async for task in executor.submit(worker, [Task(data=frame) for frame in frames]):
            num_tasks += 1
            print(
                f"Task computed: {task}, result: {task.result}, time: {task.running_time}"
            )

        print(
            f"{num_tasks} tasks computed, total time: {datetime.now() - start_time}"
        )


if __name__ == "__main__":
    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    enable_default_logger()

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        main(subnet_tag="devnet-beta.1", driver="zksync", network="rinkeby")
    )

    try:
        loop.run_until_complete(task)
    except NoPaymentAccountError as e:
        handbook_url = (
            "https://handbook.golem.network/requestor-tutorials/"
            "flash-tutorial-of-requestor-development"
        )
        print(
            f"No payment account initialized for driver `{e.required_driver}` "
            f"and network `{e.required_network}`.\n\n"
            f"See {handbook_url} on how to initialize payment accounts for a requestor node."
        )
    except KeyboardInterrupt:
        print(
            "Shutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"Shutdown completed, thank you for waiting!"
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
```
