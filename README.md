# Golem Python API

[![GitHub license](https://img.shields.io/github/license/prekucki/yapapi)](https://github.com/prekucki/yapapi/blob/master/LICENSE)
[![PyPI version](https://badge.fury.io/py/yapapi.svg)](https://badge.fury.io/py/yapapi)
[![GitHub issues](https://img.shields.io/github/issues/prekucki/yapapi)](https://github.com/prekucki/yapapi/issues)
![Continuous integration](https://github.com/prekucki/yapapi/workflows/Continuous%20integration/badge.svg)

## How to use

Rendering

```python
from yapapi.runner import Engine, Task, vm
from datetime import timedelta


async def main():
    package = await vm.repo(
        image_hash="ef007138617985ebb871e4305bc86fc97073f1ea9ab0ade9ad492ea995c4bc8b",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx, tasks):
        ctx.send_file("./scene.blend", "/golem/resource/scene.blend")
        async for task in tasks:
            frame = task.data
            ctx.begin()
            crops = [
                {
                    "outfilebasename": "out",
                    "borders_x": [0.0, 1.0],
                    "borders_y": [0.0, 1.0],
                }
            ]
            ctx.send_json(
                "/golem/work/params.json",
                {
                    "scene_file": "/golem/resource/scene.blend",
                    "resolution": (800, 600),
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
            ctx.run("/golem/entrypoints/render_entrypoint.py")
            ctx.download_file("/golem/output/out.png", f"output_{frame}.png")
            yield ctx.commit()
            # TODO: Check if job is valid
            # and reject by: task.reject_task(msg = 'invalid file')
            task.accept_task()

        ctx.log("no more frame to render")

    async with Engine(
        package=package, max_worker=10, budget=10.0, timeout=timedelta(minutes=5)
    ) as engine:
        async for progress in engine.map(
            worker, [Task(data=frame) for frame in range(1, 101)]
        ):
            print("progress=", progress)
```
