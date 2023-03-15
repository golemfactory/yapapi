#!/usr/bin/env python3
"""A requestor script for testing concurrent execution of Golem.execute_tasks().

It creates a pipeline consisting of two `Golem.execute_tasks()` calls,
with the results of the tasks computed by the first one (A) given as input data
for the tasks computed by the second one (B).
"""
import asyncio
import pathlib
import sys

from yapapi import Golem, Task
from yapapi.log import enable_default_logger
from yapapi.payload import vm


async def main():
    vm_package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    first_task = True

    async def duplicator(work_ctx, tasks):
        """Execute `echo {task.data} {task.data}` command on provider as a worker.

        The command's output is the result of the whole task.

        The first ever task computed by an instance of this worker fails.
        It succeeds when re-tried. This may be used to test re-trying tasks,
        creating new agreements and displaying summary information on task/activity
        failures.
        """
        async for task in tasks:
            nonlocal first_task

            script = work_ctx.new_script()

            if first_task:
                first_task = False
                script.run("/bin/sleep", "1")
                script.run("/command/not/found")

            script.run("/bin/sleep", "1")
            future_result = script.run("/bin/echo", task.data, task.data)
            yield script
            result = await future_result
            output = result.stdout.strip()
            task.accept_result(output)

    async with Golem(budget=1.0, subnet_tag="goth") as golem:
        # Construct a pipeline:
        #
        #   input_tasks
        #        |
        #        V
        #    [ Job ALEF ]
        #        |
        #        V
        # intermediate_tasks
        #        |
        #        V
        #    [ Job BET ]
        #        |
        #        V
        #   output_tasks

        input_tasks = [Task(s) for s in "01234567"]

        computed_input_tasks = golem.execute_tasks(
            duplicator,
            input_tasks,
            payload=vm_package,
            max_workers=1,
            job_id="ALEF",
        )

        async def intermediate_tasks():
            async for task in computed_input_tasks:
                print(f"ALEF computed task: {task.data} -> {task.result}", file=sys.stderr)
                yield Task(data=task.result)

        output_tasks = golem.execute_tasks(
            duplicator,
            intermediate_tasks(),
            payload=vm_package,
            max_workers=1,
            job_id="BET",
        )

        async for task in output_tasks:
            print(f"BET computed task: {task.data} -> {task.result}", file=sys.stderr)


if __name__ == "__main__":
    test_dir = pathlib.Path(__file__).parent.name
    enable_default_logger(log_file=f"{test_dir}.log")
    asyncio.run(main())
