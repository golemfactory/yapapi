#!/usr/bin/env python3
import argparse
import asyncio
import logging
from datetime import timedelta
from typing import AsyncIterable, List

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger, log_event_repr
from yapapi.payload import vm
from yapapi.strategy import (
    PROP_DEBIT_NOTE_INTERVAL_SEC,
    PROP_PAYMENT_TIMEOUT_SEC,
    LeastExpensiveLinearPayuMS,
    PropValueRange,
)


class ShortDebitNoteIntervalAndPaymentTimeout(LeastExpensiveLinearPayuMS):
    acceptable_prop_value_range_overrides = {
        PROP_DEBIT_NOTE_INTERVAL_SEC: PropValueRange(25, 30),
        PROP_PAYMENT_TIMEOUT_SEC: PropValueRange(60, 70),
    }


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        script = context.new_script()
        future_result = script.run("/bin/sh", "-c", "sleep 105")
        yield script
        task.accept_result(result=await future_result)


async def main():
    package = await vm.repo(image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376")

    tasks: List[Task] = [Task(data=None)]
    timeout = timedelta(hours=24)

    async with Golem(
        budget=10.0,
        strategy=ShortDebitNoteIntervalAndPaymentTimeout(),
        subnet_tag="goth",
        event_consumer=log_event_repr,
        payment_network="holesky",
    ) as golem:
        logger = logging.getLogger("yapapi")
        logger.handlers[0].setLevel(logging.DEBUG)
        async for completed in golem.execute_tasks(
            worker, tasks, payload=package, max_workers=1, timeout=timeout
        ):
            print(f"Task finished: {completed}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-file",
        default=str("mid_agreement_payments.log"),
    )
    args = parser.parse_args()
    enable_default_logger(log_file=args.log_file)

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
