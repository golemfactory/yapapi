#!/usr/bin/env python3
import asyncio
import pathlib
import sys

from yapapi import Executor, SubTask, __version__ as yapapi_version, WorkContext
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.package import vm
from datetime import timedelta

# For importing `utils.py`:
script_dir = pathlib.Path(__file__).resolve().parent
parent_directory = script_dir.parent
sys.stderr.write(f"Adding {parent_directory} to sys.path.\n")
sys.path.append(str(parent_directory))
import utils  # noqa


async def main(subnet_tag: str):
    package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, subtasks):
        scene_path = str(script_dir / "cubes.blend")
        ctx.send_file(scene_path, "/golem/resource/scene.blend")
        async for subtask in subtasks:
            frame = subtask.data
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
            # TODO: Check if job results are valid
            # and reject by: task.reject_task(reason = 'invalid file')
            subtask.accept_result(result=output_file)

        ctx.log("no more frames to render")

    # iterator over the frame indices that we want to render
    frames: range = range(0, 60, 10)
    # TODO make this dynamic, e.g. depending on the size of files to transfer
    # worst-case time overhead for initialization, e.g. negotiation, file transfer etc.
    init_overhead: timedelta = timedelta(minutes=3)

    # By passing `event_emitter=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Executor(
        package=package,
        max_workers=3,
        budget=10.0,
        timeout=init_overhead + timedelta(minutes=len(frames) * 2),
        subnet_tag=subnet_tag,
        event_emitter=log_summary(log_event_repr),
    ) as executor:

        async for subtask in executor.submit(worker, [SubTask(data=frame) for frame in frames]):
            print(
                f"{utils.TEXT_COLOR_CYAN}"
                f"Subtask computed: {subtask}, result: {subtask.output}"
                f"{utils.TEXT_COLOR_DEFAULT}"
            )


if __name__ == "__main__":
    parser = utils.build_parser("Render blender scene")
    parser.set_defaults(log_file="blender-yapapi.log")
    args = parser.parse_args()

    enable_default_logger(log_file=args.log_file)
    loop = asyncio.get_event_loop()
    subnet = args.subnet_tag
    sys.stderr.write(
        f"yapapi version: {utils.TEXT_COLOR_YELLOW}{yapapi_version}{utils.TEXT_COLOR_DEFAULT}\n"
    )
    sys.stderr.write(f"Using subnet: {utils.TEXT_COLOR_YELLOW}{subnet}{utils.TEXT_COLOR_DEFAULT}\n")
    task = loop.create_task(main(subnet_tag=args.subnet_tag))
    try:
        loop.run_until_complete(task)
    except (Exception, KeyboardInterrupt) as e:
        print(e)
        task.cancel()
        asyncio.get_event_loop().run_until_complete(task)
