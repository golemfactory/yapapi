#!/usr/bin/env python3
import asyncio
import pathlib
import random
import sys
from datetime import datetime, timedelta

examples_dir = pathlib.Path(__file__).resolve().parents[2] / "examples"
sys.path.append(str(examples_dir))

from utils import (  # noqa: E402
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
    build_parser,
)

from golem_core.core.market_api import RepositoryVmPayload  # noqa: E402

from yapapi import windows_event_loop_fix  # noqa: E402
from yapapi import Golem, NoPaymentAccountError, Task, WorkContext  # noqa: E402
from yapapi import __version__ as yapapi_version  # noqa: E402
from yapapi.log import enable_default_logger  # noqa: E402
from yapapi.rest.activity import BatchTimeoutError  # noqa: E402


async def main(subnet_tag, payment_driver=None, payment_network=None):
    package = RepositoryVmPayload(
        image_hash="a23ce2c0c29ea9711e4a293a2805700e2f0cb6450fddf9506812eb1b",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )
    tasks = [() for _ in range(10)]

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            output_file = f"output_{datetime.now()}_{random.random()}.txt"
            script = ctx.new_script(timeout=timedelta(minutes=10))
            script.run("/usr/bin/stress-ng", "--cpu", "1", "--timeout", "1")
            script.run("/golem/task.sh", "-o", "1024", "-t", "5")
            script.run("/golem/task.sh", "-f", "/golem/output/output.txt,1048576")
            script.download_file("/golem/output/output.txt", output_file)
            script.run("/golem/task.sh", "-e", "1024", "-t", "5")

            try:
                yield script
                task.accept_result(result=output_file)
            except BatchTimeoutError:
                print(
                    f"{TEXT_COLOR_RED}"
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )
                raise

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{golem.payment_driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{golem.payment_network}{TEXT_COLOR_DEFAULT}\n"
        )

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [Task(data=data) for data in tasks],
            payload=package,
            max_workers=50,
            timeout=timedelta(minutes=30),
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
    parser = build_parser("Send a drone task")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"drone-yapapi-{now}.log")
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
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        )
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
