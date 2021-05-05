#!/usr/bin/env python3
"""A requestor script for testing if multiple workers are run for an agreement."""
import asyncio
from datetime import timedelta
import logging

from yapapi import Executor, Task
from yapapi.log import enable_default_logger, log_event_repr  # noqa
from yapapi.package import vm


async def main():

    vm_package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(work_ctx, tasks):
        """Compute just one task and exit."""
        async for task in tasks:
            work_ctx.run("/bin/sleep", "1")
            yield work_ctx.commit()
            task.accept_result()
            return

    async with Executor(
        budget=10.0,
        package=vm_package,
        max_workers=1,
        subnet_tag="goth",
        timeout=timedelta(minutes=6),
        event_consumer=log_event_repr,
    ) as executor:

        tasks = [Task(data=n) for n in range(3)]
        async for task in executor.submit(worker, tasks):
            print(f"Task computed: {task}")


if __name__ == "__main__":

    enable_default_logger()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logging.getLogger("yapapi.events").addHandler(console_handler)

    asyncio.run(main())
