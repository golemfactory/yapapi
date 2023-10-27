import asyncio

from dataclasses import dataclass
from datetime import datetime, timedelta

from yapapi import Golem
from yapapi.payload import Payload
from yapapi.props import inf
from yapapi.props.base import constraint, prop
from yapapi.services import Service


from examples.utils import (
    build_parser,
    run_golem_example,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT, TEXT_COLOR_MAGENTA, format_usage,
)

RUNTIME_NAME = "ai"
CAPABILITIES = "golem.runtime.capabilities"

@dataclass
class AiPayload(Payload):

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)
    # min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=4)
    # min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=512)
    capabilities: str = constraint(CAPABILITIES, default="dummy")


class AiRuntimeService(Service):
    @staticmethod
    async def get_payload():
        return AiPayload()

    async def start(self):
        # async for script in super().start():
        #     yield script

        # every `DATE_POLL_INTERVAL` write output of `date` to `DATE_OUTPUT_PATH`
        script = self._ctx.new_script()
        script.start(
            "--model",
            "dummy_model"
        )
        yield script

    # async def run(self):
    #    # TODO run AI tasks here


async def main(subnet_tag, driver=None, network=None):
    async with Golem(
        budget=100.0,
        subnet_tag=subnet_tag,
        payment_driver=driver,
        payment_network=network,
    ) as golem:
        cluster = await golem.run_service(
            AiRuntimeService,
            num_instances=1,
        )

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        while True:
            await asyncio.sleep(3)
            print(f"instances: {instances()}")

if __name__ == "__main__":
    parser = build_parser("Run AI runtime task")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"ai-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            driver=args.payment_driver,
            network=args.payment_network,
        ),
        log_file=args.log_file,
    )
