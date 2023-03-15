#!/usr/bin/env python3
"""A requestor script used for testing agreement termination."""
import asyncio
import logging
from datetime import timedelta

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm


async def main():
    package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    first_worker = True

    async def worker(ctx: WorkContext, tasks):
        """Execute `Golem.execute_tasks()` as a worker.

        The first call to this function will produce a worker
        that sends an invalid `run` command to the provider.
        This should cause `yield script` to fail with
        `CommandExecutionError`.

        The remaining calls will just send `sleep 5` to the
        provider to simulate some work.
        """

        nonlocal first_worker
        should_fail = first_worker
        first_worker = False

        async for task in tasks:
            script = ctx.new_script()

            if should_fail:
                # Send a command that will fail on the provider
                script.run("xyz")
                yield script
            else:
                # Simulate some work
                script.run("/bin/sleep", "5")
                yield script

            task.accept_result()

    async with Golem(
        budget=10.0,
        subnet_tag="goth",
    ) as golem:
        tasks = [Task(data=n) for n in range(6)]
        async for task in golem.execute_tasks(
            worker,
            tasks,
            package,
            max_workers=1,
            timeout=timedelta(minutes=6),
        ):
            print(f"Task computed: {task}, time: {task.running_time}")

        print("All tasks computed")


if __name__ == "__main__":
    enable_default_logger(log_file="test.log")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logging.getLogger("yapapi.events").addHandler(console_handler)

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
        task.cancel()
        try:
            loop.run_until_complete(task)
            print("Shutdown completed")
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
