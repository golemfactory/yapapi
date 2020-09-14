from yapapi import enable_default_logger
from yapapi.runner import Engine, Task, vm
from yapapi.runner.ctx import WorkContext
from datetime import timedelta
import argparse
import asyncio


async def main():
    args = parse_args()
    package = await vm.repo(
        image_hash="3c436e6bdfa188e35b2881be6377e41a63062e8cd9710345757d3559",
        min_mem_gib=1.0,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):
        ctx.send_file("./cubes.blend", "/golem/resource/scene.blend")
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
            ctx.download_file(f"/golem/output/out{frame:04d}.png", f"output_{frame}.png")
            yield ctx.commit(task)
            # TODO: Check if job results are valid
            # and reject by: task.reject_task(reason = 'invalid file')
            task.accept_task()

        ctx.log("no more frames to render")

    # iterator over the frame indices that we want to render
    frames: range = range(0, 60, 10)
    # TODO make this dynamic, e.g. depending on the size of files to transfer
    # worst-case time overhead for initialization, e.g. negotiation, file transfer etc.
    init_overhead: timedelta = timedelta(minutes=3)

    async with Engine(
        package=package,
        max_workers=3,
        budget=10.0,
        timeout=init_overhead + timedelta(minutes=len(frames) * 2),
        subnet_tag=args.subnet,
        stream_output=args.stream_output,
    ) as engine:

        async for progress in engine.map(worker, [Task(data=frame) for frame in frames]):
            print("progress=", progress)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--subnet", type=str, default="testnet")
    parser.add_argument("--stream-output", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    enable_default_logger()
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    try:
        asyncio.get_event_loop().run_until_complete(task)
    except (Exception, KeyboardInterrupt) as e:
        print(e)
        task.cancel()
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.3))
