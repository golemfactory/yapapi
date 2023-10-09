#!/usr/bin/env python3
import asyncio
import filecmp
from pathlib import Path
import subprocess
from typing import AsyncIterable

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm

# docker-huge-output-command-latest-5782c9eb9a.gvmi
IMAGE_HASH = "09a273ffe606c93e139ce6a57f8a9c94e2522bc71bd15143148e3c4b"

INPUT_FILE_NAME = "huge-output-command-input.log"
OUTPUT_FILE_NAME = "huge-output-command-output.log"


def create_input_file():
    # prepare the input file the same way it is done in Dockerfile
    subprocess.run(["sh", "./create_input_file.sh"])


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        script = context.new_script()
        future_result = script.run("/bin/sh", "-c", f"cat {INPUT_FILE_NAME}")

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


def assert_input_and_output_file():
    print(f"Size of {INPUT_FILE_NAME}: {Path(INPUT_FILE_NAME).stat().st_size}")
    print(f"Size of {OUTPUT_FILE_NAME}: {Path(OUTPUT_FILE_NAME).stat().st_size}")
    assert filecmp.cmp(INPUT_FILE_NAME, OUTPUT_FILE_NAME)


if __name__ == "__main__":
    enable_default_logger(log_file="huge-output-command.log")

    create_input_file()
    asyncio.run(main())
    assert_input_and_output_file()
