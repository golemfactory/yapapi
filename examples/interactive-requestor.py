#!/usr/bin/env python3
"""A requestor script used for testing agreement termination."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from yapapi import Executor, Task, WorkContext
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.package import vm

from utils import TEXT_COLOR_CYAN, TEXT_COLOR_MAGENTA, TEXT_COLOR_DEFAULT  # type: ignore


async def main():

    package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):

        async for task in tasks:
            print(f"{TEXT_COLOR_MAGENTA}Got new task: {task.data}{TEXT_COLOR_DEFAULT}")
            ctx.run("/bin/echo", task.data)
            yield ctx.commit()
            task.accept_result()
            print(f"{TEXT_COLOR_MAGENTA}Task computed: {task.data}{TEXT_COLOR_DEFAULT}")

    async with Executor(
        package=package,
        max_workers=1,
        budget=10.0,
        timeout=timedelta(minutes=6),
        subnet_tag="goth",
        driver="zksync",
        network="rinkeby",
        event_consumer=log_summary(log_event_repr),
    ) as executor:

        async def input_iterator():

            print(
                f"{TEXT_COLOR_CYAN}"
                "Enter text to run /bin/echo on provider or empty line to finish"
                f"{TEXT_COLOR_DEFAULT}"
            )
            prompt = f"{TEXT_COLOR_CYAN}>>> {TEXT_COLOR_DEFAULT}"
            loop = asyncio.get_event_loop()

            with ThreadPoolExecutor(1) as thread_executor:
                while True:
                    await asyncio.sleep(1.0)
                    data = await loop.run_in_executor(thread_executor, input, prompt)
                    data = data.strip()
                    if not data:
                        return
                    yield Task(data)

        async for task in executor.submit(worker, input_iterator()):
            print(f"Task computed: {task}, time: {task.running_time}")


if __name__ == "__main__":

    enable_default_logger(log_file="test.log")

    asyncio.get_event_loop().run_until_complete(main())
