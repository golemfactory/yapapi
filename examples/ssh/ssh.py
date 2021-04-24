#!/usr/bin/env python3
import asyncio
import pathlib
import os
import sys

from datetime import datetime, timedelta

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
from yapapi.rest.net import SwarmBuilder

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
    app_key = os.getenv("YAGNA_APPKEY")
    if app_key is None:
        raise RuntimeError("Missing app key")

    package = await vm.repo(
        image_hash="23145d371090cfdac5715240141ad0735e1ce507b11ae2beef6d597f",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    swarm = SwarmBuilder("10.0.0.0/16")
    timeout = timedelta(minutes=30)

    async def worker(ctx: WorkContext, tasks):

        node = ctx.swarm_node
        net = node.network_id
        ip = node.ip

        print(f"""
        
{TEXT_COLOR_YELLOW}Connect with:{TEXT_COLOR_DEFAULT}
ssh -o ProxyCommand='websocat asyncstdio: ws://127.0.0.1:7465/net-api/v1/net/{net}/tcp/{ip}/22 --binary -H=Authorization:"Bearer {app_key}"' root@{ip}
   
""")

        async for task in tasks:
            try:
                ctx.run("/bin/bash", "-c", "syslogd")
                ctx.run("/bin/bash", "-c", "ssh-keygen -A")
                ctx.run("/bin/bash", "-c", "/usr/sbin/sshd")
                ctx.run("/bin/bash", "-c", "sleep 3600")
                yield ctx.commit(timeout=timeout)

                # TODO: Check if job results are valid
                # and reject by: task.reject_task(reason = 'invalid file')

                task.accept_result(result=None)
            except BatchTimeoutError:
                print(
                    f"{TEXT_COLOR_RED}"
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )
                raise

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
            swarm=swarm,
    ) as executor:

        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{executor.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{executor.network}{TEXT_COLOR_DEFAULT}\n"
        )

        num_tasks = 0
        start_time = datetime.now()
        tasks = [Task(data=None) for _ in range(2)]

        async for task in executor.submit(worker, tasks):
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
    parser = build_parser("Golem VPN SSH")
    parser.set_defaults(log_file="ssh.log")
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
