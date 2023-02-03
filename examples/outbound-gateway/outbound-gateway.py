import asyncio
from dataclasses import dataclass
from datetime import datetime

from examples.utils import build_parser, run_golem_example
from yapapi import Golem
from yapapi.payload import Payload
from yapapi.props import inf
from yapapi.props.base import constraint, prop
from yapapi.services import Service

RUNTIME_NAME = "outbound-gateway"
SOME_CUSTOM_PROPERTY = "golem.srv.app.eth.network"


@dataclass
class GatewayPayload(Payload):
    custom_property: str = prop(SOME_CUSTOM_PROPERTY)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)


class OutboundGatewayRuntimeService(Service):
    @staticmethod
    async def get_payload():
        return GatewayPayload(custom_property="whatever")


async def main(subnet_tag, driver=None, network=None):

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=driver,
        payment_network=network,
    ) as golem:
        cluster = await golem.run_service(
            OutboundGatewayRuntimeService,
        )

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        cnt = 0
        while cnt < 1:
            print(f"instances: {instances()}")
            await asyncio.sleep(3)

        cluster.stop()

        cnt = 0
        while cnt < 1 and any(s.is_available for s in cluster.instances):
            print(f"instances: {instances()}")
            await asyncio.sleep(1)

    print(f"instances: {instances()}")


if __name__ == "__main__":
    parser = build_parser("Run outbound-gateway")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"gateway-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            driver=args.payment_driver,
            network=args.payment_network,
        ),
        log_file=args.log_file,
    )
