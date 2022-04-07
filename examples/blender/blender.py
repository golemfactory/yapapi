#!/usr/bin/env python3
from datetime import datetime, timedelta
import pathlib
import sys

from yapapi import (
    Golem,
    Task,
    WorkContext,
)
from yapapi.payload import vm
from yapapi.rest.activity import BatchTimeoutError

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_MAGENTA,
    format_usage,
    run_golem_example,
    print_env_info,
)


async def main(
    subnet_tag, min_cpu_threads, payment_driver=None, payment_network=None, show_usage=False
):
    package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        # only run on provider nodes that have more than 0.5gb of RAM available
        min_mem_gib=0.5,
        # only run on provider nodes that have more than 2gb of storage space available
        min_storage_gib=2.0,
        # only run on provider nodes which a certain number of CPU threads (logical CPU cores) available
        min_cpu_threads=min_cpu_threads,
    )

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

                #   FAIL ON SOME PROVIDERS
                import random
                if (subnet_tag == "repunet" and ctx.provider_name.endswith("5")) or (
                    # subnet_tag != "repunet" and ctx.provider_name[0] < "m"
                    subnet_tag != "repunet" and random.random() > 0.9
                ):
                    script = ctx.new_script(timeout=timedelta(seconds=1))
                    script.run("/bin/sleep", "7")
                    yield script

                task.accept_result()
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

    from yapapi.contrib.strategy.rep_a1 import RepA1

    golem = Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    )

    #   Wrap the default Golem strategy
    strategy = RepA1(golem.strategy)
    golem.strategy = strategy
    golem.add_event_consumer(strategy.on_event)

    #   DEMO ADHOC (repunet)
    ids = {
        "invalid.4": "0x99db4c4d3b087284624195c4a379fcb574acb6ee",
        "invalid.6": "0xe5927c77adad86e66d1fab7152f162ed74e455f3",
        "invalid.7": "0xec6f8999e50dcd847d237e0f41b364d658ece102",
        "valid.2": "0xb08aa3b6a066337442a09b80a65c6f23a9170b99",
        "valid.3": "0x12d8b1d36aae011be3f4235fc5088787ce32098c",
        "valid.5": "0x1dc1c83790ae48e4585448d7550cebd3dc445f59",
    }
    from yapapi.contrib.strategy import ProviderFilter
    good_ids = [val for key, val in ids.items() if int(key[-1]) in (2, 3, 5)]
    if subnet_tag == 'repunet':
        golem.strategy = ProviderFilter(golem.strategy, lambda id_: id_ in good_ids)

    async with golem:
        print_env_info(golem)

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [Task(data=frame) for frame in frames],
            payload=package,
            max_workers=5,
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


if __name__ == "__main__":
    parser = build_parser("Render a Blender scene")
    parser.add_argument("--show-usage", action="store_true", help="show activity usage and cost")
    parser.add_argument(
        "--min-cpu-threads",
        type=int,
        default=1,
        help="require the provider nodes to have at least this number of available CPU threads",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"blender-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            min_cpu_threads=args.min_cpu_threads,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            show_usage=args.show_usage,
        ),
        log_file=args.log_file,
    )
