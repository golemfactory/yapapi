#!/usr/bin/env python3
"""
the requestor agent controlling and interacting with the "simple service"
"""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import pathlib
import queue
import random
import string
import sys



from yapapi import (
    NoPaymentAccountError,
    __version__ as yapapi_version,
    windows_event_loop_fix,
)
from yapapi.executor import Golem
from yapapi.executor.services import Service

from yapapi.log import enable_default_logger, log_summary, log_event_repr, pluralize  # noqa
from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)

NUM_INSTANCES = 1


class SimpleService(Service):
    STATS_PATH = "/golem/out/stats"
    PLOT_INFO_PATH = "/golem/out/plot"
    SIMPLE_SERVICE = "/golem/run/simple_service.py"

    @staticmethod
    async def get_payload():
        return await vm.repo(
        image_hash="8b11df59f84358d47fc6776d0bb7290b0054c15ded2d6f54cf634488",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def start(self):
        self._ctx.run("/golem/run/simulate_observations_ctl.py", "--start")
        yield self._ctx.commit()

    async def run(self):
        while True:
            await asyncio.sleep(10)
            self._ctx.run(self.SIMPLE_SERVICE, "--stats")  # idx 0
            self._ctx.run(self.SIMPLE_SERVICE, "--plot", "dist")  # idx 1

            future_results = yield self._ctx.commit()
            results = await future_results
            stats = results[0].stdout.strip()
            plot = results[1].stdout.strip().strip('"')

            print(f"{TEXT_COLOR_CYAN}stats: {stats}{TEXT_COLOR_DEFAULT}")

            plot_filename = (
                    "".join(random.choice(string.ascii_letters) for _ in
                            range(10)) + ".png"
            )
            print(f"{TEXT_COLOR_CYAN}downloading plot: {plot} to {plot_filename}{TEXT_COLOR_DEFAULT}")
            self._ctx.download_file(plot, str(pathlib.Path(__file__).resolve().parent / plot_filename))

            steps = self._ctx.commit()
            yield steps

    async def shutdown(self):
        self._ctx.run("/golem/run/simulate_observations_ctl.py", "--stop")
        yield self._ctx.commit()


async def main(subnet_tag, driver=None, network=None):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
        event_consumer=log_summary(log_event_repr),
    ) as golem:

        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{golem.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{golem.network}{TEXT_COLOR_DEFAULT}\n"
        )

        start_time = datetime.now()

        print(f"{TEXT_COLOR_YELLOW}starting {pluralize(NUM_INSTANCES, 'instance')}{TEXT_COLOR_DEFAULT}")

        cluster = await golem.run_service(
            SimpleService,
            num_instances=NUM_INSTANCES,
            expiration=datetime.now(timezone.utc) + timedelta(minutes=15))

        def instances():
            return [(s.provider_name, s.state.value) for s in cluster.instances]

        def still_running():
            return any([s for s in cluster.instances if s.is_available])

        while datetime.now() < start_time + timedelta(minutes=2):
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

        print(f"{TEXT_COLOR_YELLOW}stopping instances{TEXT_COLOR_DEFAULT}")
        cluster.stop()

        cnt = 0
        while cnt < 10 and still_running():
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

    print(f"instances: {instances()}")


if __name__ == "__main__":
    parser = build_parser("Test http")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"simple-service-yapapi-{now}.log")
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
