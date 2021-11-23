#!/usr/bin/env python3
"""
the requestor agent controlling and interacting with the "simple service"
"""
import asyncio
from datetime import datetime, timedelta, timezone
import pathlib
import random
import string
import sys


from yapapi import Golem
from yapapi.services import Service, ServiceState

from yapapi.log import pluralize
from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
    TEXT_COLOR_MAGENTA,
    format_usage,
    print_env_info,
    run_golem_example,
)

STARTING_TIMEOUT = timedelta(minutes=4)


class SimpleService(Service):
    SIMPLE_SERVICE = "/golem/run/simple_service.py"
    SIMPLE_SERVICE_CTL = "/golem/run/simulate_observations_ctl.py"

    def __init__(self, *args, instance_name: str, show_usage: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = instance_name
        self._show_usage = show_usage

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="8b11df59f84358d47fc6776d0bb7290b0054c15ded2d6f54cf634488",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
        )

    async def start(self):
        """handler responsible for starting the service."""

        # perform the initialization of the Service
        async for script in super().start():
            yield script

        # start the service
        script = self._ctx.new_script()
        script.run(self.SIMPLE_SERVICE_CTL, "--start")
        yield script

    async def run(self):
        # handler responsible for providing the required interactions while the service is running
        while True:
            await asyncio.sleep(10)
            script = self._ctx.new_script()
            stats_results = script.run(self.SIMPLE_SERVICE, "--stats")
            plot_results = script.run(self.SIMPLE_SERVICE, "--plot", "dist")

            yield script

            stats = (await stats_results).stdout.strip()
            plot = (await plot_results).stdout.strip().strip('"')

            print(f"{TEXT_COLOR_CYAN}stats: {stats}{TEXT_COLOR_DEFAULT}")

            plot_filename = "".join(random.choice(string.ascii_letters) for _ in range(10)) + ".png"
            print(
                f"{TEXT_COLOR_CYAN}downloading plot: {plot} to {plot_filename}{TEXT_COLOR_DEFAULT}"
            )
            script = self._ctx.new_script()
            script.download_file(plot, str(pathlib.Path(__file__).resolve().parent / plot_filename))
            yield script

            if self._show_usage:
                raw_state = await self._ctx.get_raw_state()
                usage = format_usage(await self._ctx.get_usage())
                cost = await self._ctx.get_cost()
                print(
                    f"{TEXT_COLOR_MAGENTA}"
                    f" --- {self.name} STATE: {raw_state}\n"
                    f" --- {self.name} USAGE: {usage}\n"
                    f" --- {self.name}  COST: {cost}"
                    f"{TEXT_COLOR_DEFAULT}"
                )

    async def shutdown(self):
        # handler reponsible for executing operations on shutdown
        script = self._ctx.new_script()
        script.run(self.SIMPLE_SERVICE_CTL, "--stop")
        yield script

        if self._show_usage:
            cost = await self._ctx.get_cost()
            print(f"{TEXT_COLOR_MAGENTA} --- {self.name}  COST: {cost} {TEXT_COLOR_DEFAULT}")


async def main(
    subnet_tag,
    running_time,
    payment_driver=None,
    payment_network=None,
    num_instances=1,
    show_usage=False,
):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        commissioning_time = datetime.now()

        print(
            f"{TEXT_COLOR_YELLOW}"
            f"Starting {pluralize(num_instances, 'instance')}..."
            f"{TEXT_COLOR_DEFAULT}"
        )

        # start the service

        cluster = await golem.run_service(
            SimpleService,
            instance_params=[
                {"instance_name": f"simple-service-{i+1}", "show_usage": show_usage}
                for i in range(num_instances)
            ],
            expiration=datetime.now(timezone.utc) + timedelta(minutes=120),
        )
        instances = cluster.instances

        def still_starting():
            return any(i.state in (ServiceState.pending, ServiceState.starting) for i in instances)

        # wait until instances are started

        while still_starting() and datetime.now() < commissioning_time + STARTING_TIMEOUT:
            print(f"instances: {instances}")
            await asyncio.sleep(5)

        if still_starting():
            raise Exception(f"Failed to start instances before {STARTING_TIMEOUT} elapsed :( ...")

        print(f"{TEXT_COLOR_YELLOW}All instances started :){TEXT_COLOR_DEFAULT}")

        # allow the service to run for a short while
        # (and allowing its requestor-end handlers to interact with it)

        start_time = datetime.now()

        while datetime.now() < start_time + timedelta(seconds=running_time):
            print(f"instances: {instances}")
            await asyncio.sleep(5)

        print(f"{TEXT_COLOR_YELLOW}Stopping instances...{TEXT_COLOR_DEFAULT}")
        cluster.stop()

        # wait for instances to stop

        cnt = 0
        while cnt < 10 and any(s.is_available for s in cluster.instances):
            print(f"instances: {instances}")
            await asyncio.sleep(5)

    print(f"instances: {instances}")


if __name__ == "__main__":
    parser = build_parser(
        "A very simple / POC example of a service running on Golem, utilizing the VM runtime"
    )
    parser.add_argument(
        "--running-time",
        default=120,
        type=int,
        help=(
            "How long should the instance run before the cluster is stopped "
            "(in seconds, default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--num-instances",
        type=int,
        default=1,
        help="The number of instances of the service to spawn",
    )
    parser.add_argument(
        "--show-usage",
        action="store_true",
        help="Show usage and cost of each instance while running.",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"simple-service-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            running_time=args.running_time,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            num_instances=args.num_instances,
            show_usage=args.show_usage,
        ),
        log_file=args.log_file,
    )
