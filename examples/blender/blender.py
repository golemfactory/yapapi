#!/usr/bin/env python3
import pathlib
import sys
from datetime import datetime, timedelta

from yapapi import Golem, Task, WorkContext
from yapapi.payload import vm
from yapapi.rest.activity import BatchTimeoutError

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_MAGENTA,
    TEXT_COLOR_RED,
    build_parser,
    resolve_image_hash_and_url,
    format_usage,
    print_env_info,
    run_golem_example,
)


async def start(subnet_tag, package, payment_driver=None, payment_network=None, show_usage=False):
    async def worker(ctx: WorkContext, tasks):
        script_dir = pathlib.Path(__file__).resolve().parent
        scene_path = str(script_dir / "cubes.blend")

        # Set timeout for the first script executed on the provider. Usually, 30 seconds
        # should be more than enough for computing a single frame of the provided scene,
        # however a provider may require more time for the first task if it needs to download
        # the VM image first. Once downloaded, the VM image will be cached and other tasks that use
        # that image will be computed faster.
        script = ctx.new_script(timeout=timedelta(minutes=10))
        script.upload_file(scene_path, "/golem/resource/scene.blend")

        async for task in tasks:
            frame = task.data
            crops = [{"outfilebasename": "out", "borders_x": [0.0, 1.0], "borders_y": [0.0, 1.0]}]
            script.upload_json(
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
                "/golem/work/params.json",
            )

            script.run("/golem/entrypoints/run-blender.sh")
            output_file = f"output_{frame}.png"
            script.download_file(f"/golem/output/out{frame:04d}.png", output_file)
            try:
                yield script
                # TODO: Check if job results are valid
                # and reject by: task.reject_task(reason = 'invalid file')
                task.accept_result(result=output_file)
            except BatchTimeoutError:
                print(
                    f"{TEXT_COLOR_RED}"
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )
                raise

            # reinitialize the script which we send to the engine to compute subsequent frames
            script = ctx.new_script(timeout=timedelta(minutes=1))

            if show_usage:
                raw_state = await ctx.get_raw_state()
                usage = format_usage(await ctx.get_usage())
                cost = await ctx.get_cost()
                print(
                    f"{TEXT_COLOR_MAGENTA}"
                    f" --- {ctx.provider_name} STATE: {raw_state}\n"
                    f" --- {ctx.provider_name} USAGE: {usage}\n"
                    f" --- {ctx.provider_name}  COST: {cost}"
                    f"{TEXT_COLOR_DEFAULT}"
                )

    # Iterator over the frame indices that we want to render
    frames: range = range(0, 60, 10)
    # Worst-case overhead, in minutes, for initialization (negotiation, file transfer etc.)
    # TODO: make this dynamic, e.g. depending on the size of files to transfer
    init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our demand to
    # reach the providers.
    min_timeout, max_timeout = 6, 30

    timeout = timedelta(minutes=max(min(init_overhead + len(frames) * 2, max_timeout), min_timeout))

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [Task(data=frame) for frame in frames],
            payload=package,
            max_workers=3,
            timeout=timeout,
        )
        async for task in completed_tasks:
            num_tasks += 1
            print(
                f"{TEXT_COLOR_CYAN}"
                f"Task computed: {task}, result: {task.result}, time: {task.running_time}"
                f"{TEXT_COLOR_DEFAULT}"
            )

        print(
            f"{TEXT_COLOR_CYAN}"
            f"{num_tasks} tasks computed, total time: {datetime.now() - start_time}"
            f"{TEXT_COLOR_DEFAULT}"
        )


async def create_package(args, default_image_tag):
    # Use golem/blender:latest image tag, you can overwrite this option with --image-tag or --image-hash
    if args.image_url and args.image_tag:
        raise ValueError("Only one of --image-url and --image-tag can be specified")
    if args.image_url and not args.image_hash:
        raise ValueError("--image-url requires --image-hash to be specified")
    if args.image_hash and args.image_tag:
        raise ValueError("Only one of --image-hash and --image-tag can be specified")
    elif args.image_hash:
        image_tag = None
    else:
        image_tag = args.image_tag or default_image_tag

    # resolve image by tag, hash or direct link
    package = await vm.repo(
        image_tag=image_tag,
        image_hash=args.image_hash,
        image_url=args.image_url,
        image_use_https=args.image_use_https,
        # only run on provider nodes that have more than 0.5gb of RAM available
        min_mem_gib=0.5,
        # only run on provider nodes that have more than 2gb of storage space available
        min_storage_gib=2.0,
        # only run on provider nodes which a certain number of CPU threads (logical CPU cores)
        #  available
        min_cpu_threads=args.min_cpu_threads,
    )


async def main(args):
    # Create a package using options specified in the command line
    package = await create_package(args, default_image_tag="golem/blender:latest")

    await start(
        subnet_tag=args.subnet_tag,
        package=package,
        payment_driver=args.payment_driver,
        payment_network=args.payment_network,
        show_usage=args.show_usage,
    )

if __name__ == "__main__":
    parser = build_parser("Render a Blender scene")
    parser.add_argument("--show-usage", action="store_true", help="show activity usage and cost")
    parser.add_argument(
        "--min-cpu-threads",
        type=int,
        default=1,
        help="require the provider nodes to have at least this number of available CPU threads",
    )
    parser.add_argument(
        "--image-tag", help="Image tag to use when resolving image url from Golem Registry"
    )
    parser.add_argument(
        "--image-hash", help="Image hash to use when resolving image url from Golem Registry"
    )
    parser.add_argument(
        "--image-url", help="Direct image url to use instead of resolving from Golem Registry"
    )
    parser.add_argument(
        "--image-use-https", help="Whether to use https when resolving image url from Golem Registry",
        action="store_true"
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"blender-yapapi-{now}.log")
    cmd_args = parser.parse_args()

    run_golem_example(
        main(args=cmd_args),
        log_file=cmd_args.log_file,
    )
