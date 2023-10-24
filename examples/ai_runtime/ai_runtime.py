import asyncio

from dataclasses import dataclass

from yapapi import Golem
from yapapi.payload import Payload
from yapapi.props import inf
from yapapi.props.base import constraint, prop
from yapapi.services import Service

RUNTIME_NAME = "ai"
CAPABILITIES = "golem.runtime.capabilities"

@dataclass
class CustomPayload(Payload):

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=4)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=512)
    capabilities: str = constraint(CAPABILITIES, default="dummy")


class CustomRuntimeService(Service):
    @staticmethod
    async def get_payload():
        return CustomPayload()


async def main(subnet_tag, driver=None, network=None):
    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=driver,
        payment_network=network,
    ) as golem:
        cluster = await golem.run_service(
            CustomRuntimeService,
        )

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        cnt = 0
        while cnt < 10:
            print(f"instances: {instances()}")
            await asyncio.sleep(3)

        cluster.stop()

        cnt = 0
        while cnt < 10 and any(s.is_available for s in cluster.instances):
            print(f"instances: {instances()}")
            await asyncio.sleep(1)

    print(f"instances: {instances()}")


asyncio.run(main(None))
