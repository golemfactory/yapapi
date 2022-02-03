import asyncio
from datetime import datetime
import logging
import pathlib
import re
import sys
from typing import List

from yapapi import (
    Golem,
    Task,
    WorkContext,
)
from yapapi.payload import vm
from yapapi.strategy import MarketStrategy, SCORE_REJECTED, SCORE_TRUSTED

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_YELLOW,
    TEXT_COLOR_MAGENTA,
    run_golem_example,
    print_env_info,
)

logger = logging.getLogger(__name__)

# alpine:latest
IMAGE_HASH = "d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376"

nodes_list = {}


class ScanStrategy(MarketStrategy):

    async def score_offer(self, offer, _agreements_pool=None):
        node_address = offer.issuer

        # reject nodes that we have already scanned
        if node_address in nodes_list:
            return SCORE_REJECTED

        return SCORE_TRUSTED


async def main(
    scan_size: int, max_workers: int, subnet_tag, payment_driver=None, payment_network=None
):
    payload = await vm.repo(image_hash=IMAGE_HASH)

    async def worker(ctx: WorkContext, tasks):
        print(
            f"{TEXT_COLOR_CYAN}"
            f"Getting info for {ctx.provider_id} (aka {ctx.provider_name})"
            f"{TEXT_COLOR_DEFAULT}",
        )

        assert ctx.provider_id not in nodes_list

        async for task in tasks:
            script = ctx.new_script()

            future_result = script.run("/bin/cat", "/proc/cpuinfo")
            yield script

            result = (await future_result).stdout or ""
            nodes_list[ctx.provider_id] = True

            cpu_model_match = re.search("^model name\\s+:\\s+(.*)$", result, flags=re.MULTILINE)
            if cpu_model_match:
                result = cpu_model_match.group(1)
            else:
                result = None

            task.accept_result((ctx.provider_id, ctx.provider_name, result))
            await tasks.aclose()

    async with Golem(
        budget=1,
        strategy=ScanStrategy(),
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)
        print(f"{TEXT_COLOR_YELLOW}Scanning {scan_size} nodes, using {max_workers} concurrent workers.{TEXT_COLOR_DEFAULT}")

        tasks: List[Task] = [Task(i) for i in range(scan_size)]
        async for task in golem.execute_tasks(worker, tasks, payload, max_workers=max_workers):
            print(f"{TEXT_COLOR_MAGENTA}{task.result}{TEXT_COLOR_DEFAULT}")


if __name__ == "__main__":
    parser = build_parser("Scan providers")
    parser.add_argument("--scan-size", help="Number of nodes to scan", type=int, default=5)
    parser.add_argument("--max-workers", help="Number of scans at the same time", type=int, default=3)
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"scan-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            scan_size=args.scan_size,
            max_workers=args.max_workers,
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        ),
        log_file=args.log_file,
    )
