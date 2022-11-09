#!/usr/bin/env python3
"""A requestor script for testing asynchronous generation of input tasks."""
import asyncio
from datetime import timedelta
import pathlib
import sys

from yapapi import Golem, Task
from yapapi.log import enable_default_logger, log_event_repr
from yapapi.payload import vm


async def main():

    vm_package = await vm.repo(
        image="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(work_ctx, tasks):
        async for task in tasks:
            script = work_ctx.new_script()
            print("task data:", task.data, file=sys.stderr)
            script.run("/bin/sleep", "1")
            yield script
            task.accept_result(result=task.data)

    async with Golem(
        budget=10.0,
        subnet_tag="goth",
        event_consumer=log_event_repr,
    ) as golem:

        # We use an async task generator that yields tasks removed from
        # an async queue. Each computed task will potentially spawn
        # new tasks -- this is made possible thanks to using async task
        # generator as an input to `executor.submit()`.

        task_queue = asyncio.Queue()

        # Seed the queue with the first task:
        await task_queue.put(Task(data=3))

        async def input_generator():
            """Task generator yields tasks removed from `queue`."""
            while True:
                task = await task_queue.get()
                if task.data == 0:
                    break
                yield task

        async for task in golem.execute_tasks(
            worker,
            input_generator(),
            vm_package,
            max_workers=1,
            timeout=timedelta(minutes=6),
        ):
            print("task result:", task.result, file=sys.stderr)
            for n in range(task.result):
                await task_queue.put(Task(data=task.result - 1))

        print("all done!", file=sys.stderr)


if __name__ == "__main__":
    test_dir = pathlib.Path(__file__).parent.name
    enable_default_logger(log_file=f"{test_dir}.log")
    asyncio.run(main())
