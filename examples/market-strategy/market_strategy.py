#!/usr/bin/env python3
import pathlib
import sys

from yapapi import (
    Golem,
    Task,
    WorkContext,
)
from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import build_parser, run_golem_example, print_env_info  # noqa

#   TODO
IMAGE_HASH = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"


async def main(subnet_tag, payment_driver, payment_network):
    payload = await vm.repo(image_hash=IMAGE_HASH)

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            script = ctx.new_script()
            script.run('/bin/sleep', '7')
            yield script
            task.accept_result()

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        tasks = [Task(data=None) for _ in range(3)]

        async for task in golem.execute_tasks(worker, tasks, payload, max_workers=1):
            print("EXECUTED TASK", task)


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
