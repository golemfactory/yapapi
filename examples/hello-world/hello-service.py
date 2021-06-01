#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta

# from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa

from yapapi import Golem
from yapapi.executor.services import Service
from yapapi.payload import vm

DATE_OUTPUT_PATH = "/golem/work/date.txt"
POLLING_INTERVAL_SEC = 5


class DateService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
        )

    async def start(self):
        self._ctx.run(
            "/bin/sh",
            "-c",
            f"while true; do date > {DATE_OUTPUT_PATH}; sleep {POLLING_INTERVAL_SEC}; done &",
        )
        yield self._ctx.commit()

    async def run(self):
        while True:
            await asyncio.sleep(POLLING_INTERVAL_SEC)
            self._ctx.run(
                "/bin/sh",
                "-c",
                f"cat {DATE_OUTPUT_PATH}",
            )

            future_results = yield self._ctx.commit()
            results = await future_results
            print(results[0].stdout.strip())


async def main():
    async with Golem(
        budget=1.0,
        subnet_tag="goth",
        # event_consumer=log_summary(log_event_repr),
    ) as golem:
        cluster = await golem.run_service(DateService, num_instances=1)
        start_time = datetime.now()

        while datetime.now() < start_time + timedelta(minutes=1):
            cluster_state = [(i.provider_name, i.state.value) for i in cluster.instances]
            print(cluster_state)
            await asyncio.sleep(POLLING_INTERVAL_SEC)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    # enable_default_logger(log_file="hello.log")
    loop.run_until_complete(task)
