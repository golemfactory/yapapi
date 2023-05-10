#!/usr/bin/env python3
import asyncio
import pathlib
import sys
from datetime import datetime, timedelta
from decimal import Decimal

from dataclasses import dataclass

from yapapi import Golem
from yapapi.ctx import ActivityUsage
from yapapi.log import enable_default_logger
from yapapi.payload import Payload
from yapapi.props import com, inf
from yapapi.props.base import constraint
from yapapi.services import Service, ServiceState
from yapapi.strategy import LeastExpensiveLinearPayuMS

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import build_parser


@dataclass
class CustomCounterServicePayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default="test-counters")


class CustomCounterService(Service):
    @staticmethod
    async def get_payload():
        return CustomCounterServicePayload()

    def __init__(self, *args, running_time_sec, **kwargs):
        super().__init__(*args, **kwargs)
        self._running_time_sec = running_time_sec

    async def run(self):
        start_time = datetime.now()
        print(f"service {self.id} running on '{self.provider_name}'...")
        while datetime.now() < start_time + timedelta(seconds=self._running_time_sec):
            script = self._ctx.new_script()
            script.run("sleep", "1000")
            yield script
            usage: ActivityUsage = await self._ctx.get_usage()
            cost = await self._ctx.get_cost()
            print(f"total cost so far: {cost}; activity usage: {usage.current_usage}")
            await asyncio.sleep(3)

    async def shutdown(self):
        async for s in super().shutdown():
            yield s
        print(f"service {self.id} stopped on '{self.provider_name}'")


async def main(running_time_sec, subnet_tag, driver=None, network=None):
    # necessary to set maximum price for one unit of custom counter,
    # default strategy does not take custom counter prices into account
    strategy = LeastExpensiveLinearPayuMS(
        max_price_for={
            com.Counter.CPU.value: Decimal("0.2"),
            com.Counter.TIME.value: Decimal("0.1"),
            "golem.usage.custom.counter": Decimal("0.1"),
        }
    )

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=driver,
        payment_network=network,
        strategy=strategy,
    ) as golem:
        instance_params = [{"running_time_sec": running_time_sec}]
        cluster = await golem.run_service(CustomCounterService, instance_params=instance_params)

        def print_instances():
            print(f"instances: {[{s.id, s.state.value} for s in cluster.instances]}")

        was_running = False

        while True:
            await asyncio.sleep(3)

            if was_running and all([s.state == ServiceState.terminated for s in cluster.instances]):
                print_instances()
                print("All services were successfully terminated")
                break

            if len(cluster.instances) > 0:
                print_instances()
                was_running = True


if __name__ == "__main__":
    parser = build_parser("Custom Usage Counter Example")
    parser.add_argument(
        "--running-time",
        default=30,
        type=int,
        help="How long should the the service run (in seconds, default: %(default)s)",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"custom-counters-yapapi-{now}.log")
    args = parser.parse_args()
    enable_default_logger(
        log_file=args.log_file,
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
    )
    asyncio.run(main(args.running_time, args.subnet_tag, args.payment_driver, args.payment_network))
