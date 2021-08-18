#!/usr/bin/env python3
import asyncio
from typing import AsyncIterable

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm

golem = Golem(budget=1.0, subnet_tag="devnet-beta.2")


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        context.run("/bin/sh", "-c", "date")

        future_results = yield context.commit()
        results = await future_results
        task.accept_result(result=results[-1])


async def main():
    package = await vm.repo(
        image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
    )

    tasks = [Task(data=None) for _ in range(7)]

    async for completed in golem.execute_tasks(worker, tasks, payload=package, max_workers=3):
        print(completed.result.stdout)


if __name__ == "__main__":
    enable_default_logger(log_file="hello.log")

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        task.cancel()
        try:
            loop.run_until_complete(task)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
