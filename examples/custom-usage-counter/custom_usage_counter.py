import argparse
import asyncio

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from yapapi.strategy import LeastExpensiveLinearPayuMS, DecreaseScoreForUnconfirmedAgreement

from yapapi.log import enable_default_logger

from yapapi.props.base import prop, constraint
from yapapi.props import inf, com

from yapapi import Golem
from yapapi.ctx import ActivityUsage
from yapapi.payload import Payload
from yapapi.services import Service


@dataclass
class CustomCounterServicePayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default="test-counters")
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=0.5)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=0.1)


class CustomCounterService(Service):
    @staticmethod
    async def get_payload():
        return CustomCounterServicePayload()

    async def run(self):
        print(f"service {self.id} running on {self.provider_name}...")
        while True:
            self._ctx.run("sleep", "1000")
            yield self._ctx.commit()
            usage: ActivityUsage = await self._ctx.get_usage()
            cost = await self._ctx.get_cost()
            print(
                f"total cost so far: {cost}; activity usage: {usage.current_usage} at {usage.timestamp}"
            )
            await asyncio.sleep(3)

    async def shutdown(self):
        await super().shutdown()
        print(f"service {self.id} stopped on {self.provider_name}")


async def main(subnet_tag, driver=None, network=None):

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
        cluster = await golem.run_service(
            CustomCounterService,
            num_instances=1,
        )

        def print_instances():
            print(f"instances: {[{s.id, s.state.value} for s in cluster.instances]}")

        def running():
            return any([s for s in cluster.instances if s.is_available])

        was_running = False

        while True:
            await asyncio.sleep(3)

            n = len(cluster.instances)
            if n == 0 and was_running:
                print_instances()
                break
            elif n > 0:
                print_instances()
                was_running = True
                cluster.instances[0].send_message_nowait("go")

        for svc in cluster.instances:
            cluster.stop_instance(svc)

        while running():
            print_instances()
            await asyncio.sleep(1)

    print_instances()


parser = argparse.ArgumentParser(description="Custom Usage Counter Example")
parser.add_argument("--driver", help="Payment driver name, for example `zksync`")
parser.add_argument("--network", help="Network name, for example `rinkeby`")
parser.add_argument("--subnet-tag", help="Subnet name, for example `devnet-beta.2`")
args = parser.parse_args()
asyncio.run(main(args.subnet_tag, args.driver, args.network))
