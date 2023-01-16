#!/usr/bin/env python3
import logging
import pathlib
import re
import sys
from datetime import datetime
from typing import List

from yapapi import Golem, Task, WorkContext
from yapapi.log import pluralize
from yapapi.payload import vm
from yapapi.strategy import SCORE_REJECTED, SCORE_TRUSTED, MarketStrategy

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_MAGENTA,
    TEXT_COLOR_YELLOW,
    build_parser,
    print_env_info,
    run_golem_example,
)

logger = logging.getLogger(__name__)

# alpine:latest
IMAGE_HASH = "25d5315561c3aa3b8daa1ea8cfd5a6c35f307f4347a63994c9d5f2a8"

scanned_nodes = set()


class ScanStrategy(MarketStrategy):
    async def score_offer(self, offer):
        node_address = offer.issuer

        # reject nodes that we have already scanned
        if node_address in scanned_nodes:
            return SCORE_REJECTED

        return SCORE_TRUSTED


async def main(
    scan_size: int, max_workers: int, subnet_tag, payment_driver=None, payment_network=None
):
    payload = await vm.repo(image_hash=IMAGE_HASH)

    async def worker(ctx: WorkContext, tasks):
        assert ctx.provider_id not in scanned_nodes

        async for task in tasks:
            print(
                f"{TEXT_COLOR_CYAN}"
                f"Getting info for {ctx.provider_id} (aka {ctx.provider_name})"
                f"{TEXT_COLOR_DEFAULT}",
            )
            script = ctx.new_script()

            future_result = script.run("/bin/cat", "/proc/cpuinfo")
            yield script

            result = (await future_result).stdout or ""

            cpu_model_match = re.search("^model name\\s+:\\s+(.*)$", result, flags=re.MULTILINE)
            if cpu_model_match:
                result = cpu_model_match.group(1)
            else:
                result = None

            # add the node to the set so we don't end up signing another agreement with it
            scanned_nodes.add(ctx.provider_id)

            # and accept the result (pass the result to the loop in `main`)
            task.accept_result((ctx.provider_id, ctx.provider_name, result))

            # as we don't really want the engine to execute any more tasks on this node,
            # we signal the parent generator to exit and through that
            # also request termination of the worker and the agreement
            #
            # issuing a `break` here instead will usually not do what the user is expecting,
            # as the parent generator would just exit cleanly without notifying the
            # engine and there's nothing stopping the engine from re-launching the activity/worker
            # on the same agreement
            await tasks.aclose()

    async with Golem(
        budget=1,
        strategy=ScanStrategy(),
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)
        print(
            f"{TEXT_COLOR_YELLOW}Scanning {pluralize(scan_size, 'node')}, "
            f"using {pluralize(max_workers, 'concurrent worker')}.{TEXT_COLOR_DEFAULT}"
        )

        tasks: List[Task] = [Task(i) for i in range(scan_size)]
        async for task in golem.execute_tasks(worker, tasks, payload, max_workers=max_workers):
            print(f"{TEXT_COLOR_MAGENTA}{task.result}{TEXT_COLOR_DEFAULT}")


if __name__ == "__main__":
    parser = build_parser("Scan providers")
    parser.add_argument("--scan-size", help="Number of nodes to scan", type=int, default=5)
    parser.add_argument(
        "--max-workers", help="Number of scans at the same time", type=int, default=3
    )
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
