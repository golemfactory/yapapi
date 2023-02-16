#!/usr/bin/env python3
import asyncio
from typing import AsyncIterable

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm

# docker-huge-output-command-latest-1b1b56276d.gvmi
IMAGE_HASH = "aedd034143b77d80b36ad6ee0b289a4fea4faa8e0232cef54b102485"

FILE_NAME = "hello-from-golem.txt"
OUTPUT_FILE_NAME = "huge-output-command-output.log"


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        script = context.new_script()
        future_result = script.run("/bin/sh", "-c", f"cat {FILE_NAME}")

        yield script

        task.accept_result(result=await future_result)


async def main():
    package = await vm.repo(image_hash=IMAGE_HASH)

    tasks = [Task(data=None)]

    async with Golem(budget=1.0, subnet_tag="public") as golem:
        async for completed in golem.execute_tasks(worker, tasks, payload=package):
            result = completed.result.stdout

    with open(OUTPUT_FILE_NAME, "w") as file:
        file.write(result)


if __name__ == "__main__":
    enable_default_logger(log_file="huge-output-command.log")

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
