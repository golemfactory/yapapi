# Golem Python API

[![Tests - Status](https://img.shields.io/github/workflow/status/golemfactory/yapapi/Continuous%20integration/master?label=tests)](https://github.com/golemfactory/yapapi/actions?query=workflow%3A%22Continuous+integration%22+branch%3Amaster)
![PyPI - Status](https://img.shields.io/pypi/status/yapapi)
[![PyPI version](https://badge.fury.io/py/yapapi.svg)](https://badge.fury.io/py/yapapi)
[![GitHub license](https://img.shields.io/github/license/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/issues)

## What's Golem, btw?

[Golem](https:://golem.network) is a global, open-source, decentralized supercomputer 
that anyone can access. It connects individual machines - be that laptops, home PCs or 
even data centers - to form a vast network, the purpose of which is to provide a way to 
distribute computations to its provider nodes and allow requestors to utilize its unique 
potential - which can lie in its combined computing power, the geographical distrubution 
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
do that in the [yagna repository](https://github.com/golemfactory/yagna) in our 
[handbook](https://handbook.golem.network)), what you need to do is:
* *prepare your payload* - this needs to be a Docker image containing your application
  that will be executed on the provider's end. This image needs to have its volumes
  mapped in a way that will allow the supervisor module to exchange data (write and 
  read files) with it. This image needs to be packed and uploaded into golem's image repository 
  using or dedicated tool - [`gvmkit-build`](https://pypi.org/project/gvmkit-build/). 
* *create your requestor agent* - this is where `yapapi` comes in. Utilizing our high-level
  API, the creation of a requestor agent should be straighforward and require minimal effort.
  You can use examples contained in this repository (blender and hashcat) as references.

### Example

An example Golem application, using a Docker image containing the Blender renderer:

```python
import asyncio

from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.runner import Engine, Task, vm
from yapapi.runner.ctx import WorkContext
from datetime import timedelta


async def main(subnet_tag="testnet"):
    package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):
        ctx.send_file("./scene.blend", "/golem/resource/scene.blend")
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
            yield ctx.commit()
            task.accept_task(result=output_file)

        ctx.log("no more frames to render")

    # iterator over the frame indices that we want to render
    frames: range = range(0, 60, 10)
    init_overhead: timedelta = timedelta(minutes=3)

    # By passing `event_emitter=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Engine(
        package=package,
        max_workers=3,
        budget=10.0,
        timeout=init_overhead + timedelta(minutes=len(frames) * 2),
        subnet_tag=subnet_tag,
        event_emitter=log_summary(),
    ) as engine:

        async for task in engine.map(worker, [Task(data=frame) for frame in frames]):
            print(f"\033[36;1mTask computed: {task}, result: {task.output}\033[0m")


enable_default_logger()
loop = asyncio.get_event_loop()
task = loop.create_task(main(subnet_tag="devnet-alpha.2"))
try:
    asyncio.get_event_loop().run_until_complete(task)
except (Exception, KeyboardInterrupt) as e:
    print(e)
    task.cancel()
    asyncio.get_event_loop().run_until_complete(task)
```
