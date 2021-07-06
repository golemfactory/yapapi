#!/usr/bin/env python3
"""A simple requestor that creates a cluster with a single service instance.

The service is designed so that the first instance created (and only the first one)
sends an error command in its `start()` method which results in the instance's
termination before going to the `running` state.
"""
import asyncio
from datetime import datetime

from yapapi import Golem
from yapapi.services import Service

from yapapi.log import enable_default_logger, log_summary, log_event_repr, pluralize  # noqa
from yapapi.payload import vm


instances_started = 0
instances_running = 0


class FirstInstanceFailsToStart(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="8b11df59f84358d47fc6776d0bb7290b0054c15ded2d6f54cf634488",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
        )

    async def start(self):

        global instances_started
        instances_started += 1

        await asyncio.sleep(1)

        if instances_started == 1:
            self._ctx.run("/no/such/command")
        else:
            self._ctx.run("/bin/echo", "STARTING")
        future_results = yield self._ctx.commit()
        results = await future_results
        print(f"{results[-1].stdout.strip()}")

    async def run(self):

        global instances_running
        instances_running += 1

        await asyncio.sleep(1)
        self._ctx.run("/bin/echo", "RUNNING")
        future_results = yield self._ctx.commit()
        results = await future_results
        print(f"{results[-1].stdout.strip()}")


async def main():

    async with Golem(budget=1.0, subnet_tag="goth") as golem:

        print("Starting cluster...")
        await golem.run_service(FirstInstanceFailsToStart)

        while not instances_running:
            print("Waiting for an instance...")
            await asyncio.sleep(5)


if __name__ == "__main__":

    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    enable_default_logger(
        log_file=f"test-instance-restart-{now}.log",
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
