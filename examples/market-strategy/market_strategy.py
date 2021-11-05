#!/usr/bin/env python3
import pathlib
import sys

from yapapi import (
    Golem,
    Task,
    WorkContext,
)
from yapapi.payload import vm
from most_efficient_provider_strategy import MostEfficientProviderStrategy

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import build_parser, run_golem_example, print_env_info  # noqa

#   Image based on pure python:3.8-alpine
IMAGE_HASH = "5c385688be6ed4e339a43d8a68bfb674d60951b4970448ba20d1934d"

#   This is the task we'll be running
TASK_CMD = ['/usr/local/bin/python', '-c', 'for i in range(10000000): i * 7']


async def main(subnet_tag, payment_driver, payment_network):
    payload = await vm.repo(image_hash=IMAGE_HASH)

    strategy = MostEfficientProviderStrategy()

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            script = ctx.new_script()
            result = script.run('/usr/bin/time', '-p', *TASK_CMD)
            yield script

            real_time_str = result.result().stderr.split()[1]
            real_time = float(real_time_str)

            strategy.save_execution_time(ctx.provider_id, real_time)
            print("TASK EXECUTED", ctx.provider_name, ctx.provider_id, real_time)

            task.accept_result()

    async with Golem(
        budget=10,
        strategy=strategy,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        for i in range(10000):
            async for task in golem.execute_tasks(worker, [Task(None)], payload, max_workers=1):
                pass


if __name__ == "__main__":
    parser = build_parser("Render a Blender scene")
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
