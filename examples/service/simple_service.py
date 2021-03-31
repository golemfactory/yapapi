#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta
import json
import pathlib
import random
import string
import sys
import tempfile
import time


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

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)


async def main(subnet_tag, driver=None, network=None):
    package = await vm.repo(
        image_hash="8b11df59f84358d47fc6776d0bb7290b0054c15ded2d6f54cf634488",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def service(ctx: WorkContext, tasks):
        STATS_PATH = "/golem/out/stats"
        PLOT_INFO_PATH = "/golem/out/plot"
        SIMPLE_SERVICE = "/golem/run/simple_service.py"

        ctx.run("/golem/run/simulate_observations_ctl.py", "--start")
        ctx.send_bytes("/golem/in/get_stats.sh", f"{SIMPLE_SERVICE} --stats > {STATS_PATH}".encode())
        ctx.send_bytes("/golem/in/get_plot.sh", f"{SIMPLE_SERVICE} --plot dist > {PLOT_INFO_PATH}".encode())

        yield ctx.commit()

        plots_to_download = []

        def on_plot(out: bytes):
            fname = json.loads(out.strip())
            print(f"{TEXT_COLOR_CYAN}plot: {fname}{TEXT_COLOR_DEFAULT}")
            plots_to_download.append(fname)

        try:
            while True:
                await asyncio.sleep(10)

                ctx.run("/bin/sh", "/golem/in/get_stats.sh")
                ctx.download_bytes(STATS_PATH, lambda out: print(f"{TEXT_COLOR_CYAN}stats: {out}{TEXT_COLOR_DEFAULT}"))
                ctx.run("/bin/sh", "/golem/in/get_plot.sh")
                ctx.download_bytes(PLOT_INFO_PATH, on_plot)
                yield ctx.commit()

                for plot in plots_to_download:
                    test_filename = "".join(random.choice(string.ascii_letters) for _ in range(10)) + ".png"
                    ctx.download_file(plot, pathlib.Path(__file__).resolve().parent / test_filename)
                yield ctx.commit()

        except KeyboardInterrupt:
            ctx.run("/golem/run/simulate_observations_ctl.py", "--stop")
            yield ctx.commit()

        finally:
            async for task in tasks:
                task.accept_result()

    # Worst-case overhead, in minutes, for initialization (negotiation, file transfer etc.)
    # TODO: make this dynamic, e.g. depending on the size of files to transfer
    init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our demand to
    # reach the providers.
    min_timeout, max_timeout = 6, 30

    timeout = timedelta(minutes=min_timeout)

    # By passing `event_consumer=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Executor(
        package=package,
        max_workers=3,
        budget=10.0,
        timeout=timeout,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
        event_consumer=log_summary(log_event_repr),
    ) as executor:

        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{executor.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{executor.network}{TEXT_COLOR_DEFAULT}\n"
        )

        num_tasks = 0
        start_time = datetime.now()

        async for task in executor.submit(service, [Task(data=None)]):
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
    parser = build_parser("Test http")
    parser.set_defaults(log_file="service-yapapi.log")
    args = parser.parse_args()

    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    enable_default_logger(
        log_file=args.log_file,
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        main(subnet_tag=args.subnet_tag, driver=args.driver, network=args.network)
    )

    try:
        loop.run_until_complete(task)
    except NoPaymentAccountError as e:
        handbook_url = (
            "https://handbook.golem.network/requestor-tutorials/"
            "flash-tutorial-of-requestor-development"
        )
        print(
            f"{TEXT_COLOR_RED}"
            f"No payment account initialized for driver `{e.required_driver}` "
            f"and network `{e.required_network}`.\n\n"
            f"See {handbook_url} on how to initialize payment accounts for a requestor node."
            f"{TEXT_COLOR_DEFAULT}"
        )
    except KeyboardInterrupt:
        print(
            f"{TEXT_COLOR_YELLOW}"
            "Shutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
            f"{TEXT_COLOR_DEFAULT}"
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
