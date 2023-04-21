#!/usr/bin/env python3
import itertools
import pathlib
import sys
from collections import defaultdict

from golem_core.core.market_api import RepositoryVmPayload

from yapapi import Golem, Task, WorkContext
from yapapi.strategy import SCORE_TRUSTED, MarketStrategy

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import build_parser, print_env_info, run_golem_example  # noqa

#   Image based on pure python:3.8-alpine
IMAGE_HASH = "5c385688be6ed4e339a43d8a68bfb674d60951b4970448ba20d1934d"

#   This is the task we'll be running on the provider
TASK_CMD = ["/usr/local/bin/python", "-c", "for i in range(10000000): i * 7"]


class FastestProviderStrategy(MarketStrategy):
    """Strategy that ignores all offer parameters (including pricing) and just selects the fastest \
    provider.

    Decision algorithm:
    * we always try any new provider
    * if there are no new providers, we select the one with shortest average execution time in past
      runs
    """

    def __init__(self):
        self.history = defaultdict(list)

    async def score_offer(self, offer):
        provider_id = offer.issuer
        previous_runs = self.history[provider_id]

        if not previous_runs:
            score = SCORE_TRUSTED
        else:
            avg_time = sum(previous_runs) / len(previous_runs)
            score = SCORE_TRUSTED - avg_time

        if offer.is_draft:
            #   Non-draft offers are not shown to limit the number of lines printed
            if previous_runs:
                print(
                    f"Scored known provider: {provider_id}: "
                    f"{score} ({len(previous_runs)} runs, avg time {avg_time})"
                )
            else:
                print(f"Found new provider: {provider_id}, default score {SCORE_TRUSTED}")

        return score

    def save_execution_time(self, provider_id: str, time: float):
        self.history[provider_id].append(time)


async def main(subnet_tag, payment_driver, payment_network):
    payload = RepositoryVmPayload(image_hash=IMAGE_HASH)

    strategy = FastestProviderStrategy()

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            script = ctx.new_script()
            future_result = script.run("/usr/bin/time", "-p", *TASK_CMD)
            yield script

            real_time_str = future_result.result().stderr.split()[1]
            real_time = float(real_time_str)

            strategy.save_execution_time(ctx.provider_id, real_time)
            print("TASK EXECUTED", ctx.provider_name, ctx.provider_id, real_time)

            task.accept_result()

            #   We want to test as many different providers as possible, so here we tell
            #   the Golem engine to stop computations in this agreement (and thus to look
            #   for a new agreement, maybe with a new provider).
            await tasks.aclose()

    async with Golem(
        budget=10,
        strategy=strategy,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        #   Task generator that never ends
        tasks = (Task(None) for _ in itertools.count(1))
        async for task in golem.execute_tasks(worker, tasks, payload, max_workers=1):
            pass


if __name__ == "__main__":
    parser = build_parser("Select fastest provider using a simple reputation-based market strategy")
    parser.set_defaults(log_file="market-strategy-example.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        ),
        log_file=args.log_file,
    )
