#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta
from typing import AsyncIterable

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        script = context.new_script(timeout=timedelta(minutes=1))
        future_result = script.run("/bin/bash", "/chainlink/run.sh")

        yield script

        task.accept_result(result=await future_result)


async def main():
    package = await vm.repo(
        image_hash="a0ae0ded4cd75cf4a58814711704bf46ef8b154225acdd67568f0974",
    )

    tasks = [Task(data=None)]

    async with Golem(budget=1.0, subnet_tag="devnet-beta") as golem:
        async for completed in golem.execute_tasks(worker, tasks, payload=package):
            print(completed.result.stdout)


if __name__ == "__main__":
    enable_default_logger(log_file="hello.log")

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
