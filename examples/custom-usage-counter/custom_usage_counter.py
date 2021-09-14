#!/usr/bin/env python3
import argparse
import asyncio

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from yapapi.strategy import LeastExpensiveLinearPayuMS, DecreaseScoreForUnconfirmedAgreement

from yapapi.log import enable_default_logger

from yapapi.props.base import prop, constraint
from yapapi.props import inf, com

from yapapi import Golem
from yapapi.ctx import ActivityUsage
from yapapi.payload import Payload
from yapapi.services import Service, ServiceState


@dataclass
class CustomCounterServicePayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default="test-counters")
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=0.5)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=0.1)


class CustomCounterService(Service):
    @staticmethod
    async def get_payload():
        return CustomCounterServicePayload()

    def __init__(self, *args, running_time_sec, **kwargs):
        super().__init__(*args, **kwargs)
        self._running_time_sec = running_time_sec

    async def start(self):
        self._ctx.run("sleep", "1")
        yield self._ctx.commit()

    async def run(self):
        start_time = datetime.now()
        print(f"service {self.id} running on '{self.provider_name}'...")
        while datetime.now() < start_time + timedelta(seconds=self._running_time_sec):
            self._ctx.run("sleep", "1000")
            yield self._ctx.commit()
            usage: ActivityUsage = await self._ctx.get_usage()
            cost = await self._ctx.get_cost()
            print(f"total cost so far: {cost}; activity usage: {usage.current_usage}")
            await asyncio.sleep(3)

    async def shutdown(self):
        async for s in super().shutdown():
            yield s
        print(f"service {self.id} stopped on '{self.provider_name}'")


async def main(running_time_sec, subnet_tag, driver=None, network=None):

    enable_default_logger(
        log_file=str(Path(__file__).parent / "custom-counters.log"),
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
    )

    strategy = LeastExpensiveLinearPayuMS(
        max_price_for={
            com.Counter.CPU.value: Decimal("0.2"),
            com.Counter.TIME.value: Decimal("0.1"),
            "golem.usage.custom.counter": Decimal("0.1"),
        }
    )
    strategy = DecreaseScoreForUnconfirmedAgreement(strategy, 0.5)

    async with Golem(
        budget=10.0, subnet_tag=subnet_tag, driver=driver, network=network, strategy=strategy
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


parser = argparse.ArgumentParser(description="Custom Usage Counter Example")
parser.add_argument("--driver", help="Payment driver name, for example `zksync`")
parser.add_argument("--network", help="Network name, for example `rinkeby`")
parser.add_argument("--subnet-tag", help="Subnet name, for example `devnet-beta.2`")
parser.add_argument(
    "--running-time",
    default=30,
    type=int,
    help="How long should the the service run (in seconds, default: %(default)s)",
)
args = parser.parse_args()
asyncio.run(main(args.running_time, args.subnet_tag, args.driver, args.network))
