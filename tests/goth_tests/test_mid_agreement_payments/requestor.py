#!/usr/bin/env python3
import asyncio
import logging
from typing import AsyncIterable
from datetime import timedelta

from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger, log_event_repr
from yapapi.payload import vm
from yapapi.props.builder import DemandBuilder
from yapapi.strategy import LeastExpensiveLinearPayuMS, WrappingMarketStrategy


class ShortDebitNoteIntervalAndPaymentTimeout(WrappingMarketStrategy):
    def __init__(self, base_strategy):
        super().__init__(base_strategy)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        await self.base_strategy.decorate_demand(demand)
        demand.ensure(f"(PROP_DEBIT_NOTE_INTERVAL_SEC=15)")
        demand.ensure(f"(PROP_PAYMENT_TIMEOUT_SEC=20)")
        demand.ensure(f"(PROP_DEBIT_NOTE_ACCEPTANCE_TIMEOUT=10)")


async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
    async for task in tasks:
        script = context.new_script()
        future_result = script.run("/bin/sh", "-c", "sleep 100")
        yield script
        task.accept_result(result=await future_result)


async def main():
    package = await vm.repo(image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376")

    tasks = [Task(data=None)]
    timeout = timedelta(hours=24)

    async with Golem(
        budget=10.0,
        strategy=ShortDebitNoteIntervalAndPaymentTimeout(LeastExpensiveLinearPayuMS()),
        subnet_tag="goth",
        event_consumer=log_event_repr,
    ) as golem:
        logger = logging.getLogger("yapapi")
        logger.handlers[0].setLevel(logging.DEBUG)
        async for completed in golem.execute_tasks(worker, tasks, payload=package, timeout=timeout):
            print(f"Task finished: {completed}.")


if __name__ == "__main__":
    enable_default_logger(log_file="mid_agreement_payments.log")

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
