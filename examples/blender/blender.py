#!/usr/bin/env python3
import asyncio
import pathlib
import sys

from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.runner import Engine, Task, vm
from yapapi.runner.ctx import WorkContext, CaptureContext
from datetime import timedelta

# For importing `utils.py`:
script_dir = pathlib.Path(__file__).resolve().parent
parent_directory = script_dir.parent
sys.stderr.write(f"Adding {parent_directory} to sys.path.\n")
sys.path.append(str(parent_directory))
import utils  # noqa


async def main(subnet_tag="testnet", capture_ctx=None):
    package = await vm.repo(
        image_hash="3c436e6bdfa188e35b2881be6377e41a63062e8cd9710345757d3559",
        min_mem_gib=1.0,
        min_storage_gib=2.0,
    )

    if not capture_ctx:
        capture_ctx = CaptureContext.all()

    async def worker(ctx: WorkContext, tasks):
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
            ctx.run("/golem/entrypoints/run-blender.sh", stdout=capture_ctx, stderr=capture_ctx)
            output_file = f"output_{frame}.png"
            ctx.download_file(f"/golem/output/out{frame:04d}.png", output_file)
            yield ctx.commit()
            # TODO: Check if job results are valid
            # and reject by: task.reject_task(reason = 'invalid file')
            task.accept_task(result=output_file)

        ctx.log("no more frames to render")

    # iterator over the frame indices that we want to render
    frames: range = range(0, 60, 10)
    # TODO make this dynamic, e.g. depending on the size of files to transfer
    # worst-case time overhead for initialization, e.g. negotiation, file transfer etc.
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
        stream_output=capture_ctx.is_streaming(),
    ) as engine:

        async for task in engine.map(worker, [Task(data=frame) for frame in frames]):
            print(f"\033[36;1mTask computed: {task}, result: {task.output}\033[0m")


def build_capture_ctx(mode=None, limit=None, fmt=None):
    if mode in (None, "all"):
        return CaptureContext.all(fmt)
    elif mode == "stream":
        return CaptureContext.stream(limit, fmt)
    elif mode == "head":
        return CaptureContext.head(limit, fmt)
    elif mode == "tail":
        return CaptureContext.tail(limit, fmt)
    elif mode == "headTail":
        return CaptureContext.head_tail(limit, fmt)
    raise RuntimeError(f"Invalid output capture mode: {mode}")


if __name__ == "__main__":
    import pathlib
    import sys

    parser = utils.build_parser("Render blender scene")
    parser.add_argument("--capture-mode", type=str, choices=["stream", "all", "head", "tail", "headTail"])
    parser.add_argument("--capture-limit", type=int, default=None)
    parser.add_argument("--capture-format", type=str, choices=["str", "bin"])
    args = parser.parse_args()
    capture_ctx = build_capture_ctx(args.capture_mode, args.capture_limit, args.capture_format)

    enable_default_logger(level=args.log_level)
    loop = asyncio.get_event_loop()
    task = loop.create_task(main(subnet_tag=args.subnet_tag, capture_ctx=capture_ctx))
    try:
        asyncio.get_event_loop().run_until_complete(task)
    except (Exception, KeyboardInterrupt) as e:
        print(e)
        task.cancel()
        asyncio.get_event_loop().run_until_complete(task)
