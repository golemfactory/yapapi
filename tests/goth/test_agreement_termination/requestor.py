#!/usr/bin/env python3
"""A requestor script used for testing agreement termination."""
import asyncio
from datetime import timedelta
import logging

from yapapi import Executor, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm


async def main():

    package = vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    first_worker = True

    async def worker(ctx: WorkContext, tasks):
        """A worker function for `Executor.submit()`.

        The first call to this function will produce a worker
        that sends an invalid `run` command to the provider.
        This should cause `yield ctx.commit()` to fail with
        `CommandExecutionError`.

        The remaining calls will just send `sleep 5` to the
        provider to simulate some work.
        """

        nonlocal first_worker
        should_fail = first_worker
        first_worker = False

        async for task in tasks:

            if should_fail:
                # Send a command that will fail on the provider
                ctx.run("xyz")
                yield ctx.commit()
            else:
                # Simulate some work
                ctx.run("/bin/sleep", "5")
                yield ctx.commit()

            task.accept_result()

    async with Executor(
        package=package,
        max_workers=1,
        budget=10.0,
        timeout=timedelta(minutes=6),
        subnet_tag="goth",
        driver="zksync",
        network="rinkeby",
    ) as executor:

        tasks = [Task(data=n) for n in range(6)]
        async for task in executor.submit(worker, tasks):
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
