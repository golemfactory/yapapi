#!/usr/bin/env python3
"""A simple requestor that creates a cluster with a single service instance.

The service is designed so that the first instance created (and only the first one)
sends an error command in its `start()` method which results in the instance's
termination before going to the `running` state.
"""
import asyncio
import sys
from datetime import datetime

from yapapi import Golem
from yapapi.log import enable_default_logger, log_event_repr, log_summary, pluralize  # noqa
from yapapi.payload import vm
from yapapi.services import Service

instances_started = 0
instances_running = 0
instances_stopped = 0


def log(*args):
    # Like `print` but outputs to stderr to avoid buffering
    print(*args, file=sys.stderr)


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

        async for script in super().start():
            yield script

        script = self._ctx.new_script()
        future_result = script.run("/bin/echo", "STARTING", str(instances_started + 1))
        yield script
        result = await future_result
        log(f"{result.stdout.strip()}")
        instances_started += 1

        if instances_started == 1:
            script = self._ctx.new_script()
            future_result = script.run("/no/such/command")
            yield script
            await future_result

        if instances_started > 2:
            # Wait for the stop signal here
            await asyncio.sleep(30)

    async def run(self):
        global instances_running
        instances_running += 1

        script = self._ctx.new_script()
        future_result = script.run("/bin/echo", "RUNNING", str(instances_started))
        yield script
        result = await future_result
        log(f"{result.stdout.strip()}")

        script = self._ctx.new_script()
        future_result = script.run("/no/such/command")
        yield script
        await future_result

    async def shutdown(self):
        global instances_stopped

        log("STOPPING", instances_started)
        if False:
            yield
        instances_stopped += 1


async def main():
    async with Golem(budget=1.0, subnet_tag="goth", payment_network="rinkeby") as golem:
        # Start a cluster with a single service.
        # The first instance will fail before reaching the `running` state
        # due to an error. Another instance should be spawned in its place.

        log("Starting cluster...")
        await golem.run_service(FirstInstanceFailsToStart)

        # This another instance should get to `running` state.

        while not instances_running:
            log("Waiting for an instance...")
            await asyncio.sleep(2)

        # And then stop.

        while not instances_stopped:
            log("Waiting until the instance stops...")
            await asyncio.sleep(2)

        # Then we start another cluster with a single instance.
        # This time the instance is stopped by a signal before it reaches the `running` state,
        # but in that case the cluster should not spawn another instance.

        log("Starting another cluster...")
        cluster = await golem.run_service(FirstInstanceFailsToStart)

        while instances_started < 3:
            log("Waiting for another instance...")
            await asyncio.sleep(2)

        assert [i for i in cluster.instances if i.is_available]

        log("Closing the second cluster...")
        cluster.stop()

        while [i for i in cluster.instances if i.is_available]:
            log("Waiting for the cluster to stop...")
            await asyncio.sleep(2)

        log("Cluster stopped")


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
