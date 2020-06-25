from yapapi.runner import Engine, Task, vm
from yapapi.runner.ctx import WorkContext
from datetime import timedelta
import asyncio


async def main():
    package = await vm.repo(
        image_hash="6aa255e26ff0877e58981b5f2cf15c6733670c414d6c96fc3e40442b",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):
        ctx.send_file("./scene.blend", "/golem/resource/scene.blend")
        async for task in tasks:
            frame = task.data
            ctx.begin()
            crops = [{"outfilebasename": "out", "borders_x": [0.0, 1.0], "borders_y": [0.0, 1.0]}]
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
        package=package, max_workers=10, budget=10.0, timeout=timedelta(minutes=5)
    ) as engine:
        async for progress in engine.map(worker, [Task(data=frame) for frame in range(1, 101)]):
            print("progress=", progress)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    try:
        asyncio.get_event_loop().run_until_complete(task)
    except KeyboardInterrupt:
        task.cancel()
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.3))
