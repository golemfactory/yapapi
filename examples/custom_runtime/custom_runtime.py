import asyncio

from dataclasses import dataclass

from yapapi.props.base import prop, constraint
from yapapi.props import inf

from yapapi.payload import Payload
from yapapi import Golem
from yapapi.services import Service


RUNTIME_NAME = "my-runtime"
SOME_CUSTOM_PROPERTY = "golem.srv.app.eth.network"

# The `CustomPayload` in this example is an arbitrary definition of some demand
# that the requestor might wish to be fulfilled by the providers on the Golem network
#
# This payload must correspond with a runtime running on providers, either a runtime
# written by some other party, or developed alongside its requestor counterpart using
# the Runtime SDK (https://github.com/golemfactory/ya-runtime-sdk/)
#
# It is up to the author of said runtime to define any additional properties that would
# describe the requestor's demand and `custom_property` is just an example.

@dataclass
class CustomPayload(Payload):
    custom_property: str = prop(SOME_CUSTOM_PROPERTY)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)


class CustomRuntimeService(Service):
    @staticmethod
    async def get_payload():
        return CustomPayload(custom_property="whatever")


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
        while cnt < 10 and cluster.has_active_instances:
            print(f"instances: {instances()}")
            await asyncio.sleep(1)

    print(f"instances: {instances()}")


asyncio.run(main(None))
