import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import base64

from examples.utils import (
    build_parser,
    run_golem_example,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT, TEXT_COLOR_MAGENTA, format_usage,
)
from yapapi import Golem
from yapapi.payload import Payload
from yapapi.props import inf
from yapapi.props.base import constraint, prop
from yapapi.services import Service

RUNTIME_NAME = "outbound-gateway"
MANIFEST_PROPERTY = "golem.srv.comp.payload.@tag"


@dataclass
class GatewayPayload(Payload):
    manifest_property: str = prop(MANIFEST_PROPERTY)
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)


class OutboundGatewayRuntimeService(Service):
    @staticmethod
    async def get_payload():
        manifest = open("manifest.json", "rb").read()
        manifest = base64.b64encode(manifest).decode("utf-8")
        return GatewayPayload(manifest_property=manifest)

    async def run(self):
        # Perform mock command on runtime to check if it works
        script = self._ctx.new_script()
        results = script.run("test")

        yield script

        result = (await results).stdout.strip()
        print(f"{TEXT_COLOR_CYAN}{result}{TEXT_COLOR_DEFAULT}")

        while True:
            await asyncio.sleep(10)

            raw_state = await self._ctx.get_raw_state()
            usage = format_usage(await self._ctx.get_usage())
            cost = await self._ctx.get_cost()
            print(
                f"{TEXT_COLOR_MAGENTA}"
                f" --- {self.provider_name} STATE: {raw_state}\n"
                f" --- {self.provider_name} USAGE: {usage}\n"
                f" --- {self.provider_name}  COST: {cost}"
                f"{TEXT_COLOR_DEFAULT}"
            )


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
