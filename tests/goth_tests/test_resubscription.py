"""Test if subscription expiration is handled correctly by Golem"""
from datetime import timedelta
import logging
import os
from pathlib import Path
import time
from typing import Dict, Set, Type, List
from unittest.mock import Mock

import colors
import pytest

from goth.assertions import EventStream
from goth.assertions.monitor import EventMonitor
from goth.assertions.operators import eventually

from goth.configuration import load_yaml, Override
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

from yapapi import Golem, Task
from yapapi.events import (
    Event,
    JobStarted,
    JobFinished,
    SubscriptionCreated,
)
from yapapi.log import enable_default_logger
from yapapi.payload import vm
import yapapi.rest.market

import ya_market.api.requestor_api
from ya_market import ApiException

logger = logging.getLogger("goth.test")

SUBSCRIPTION_EXPIRATION_TIME = 5
"""Number of seconds after which a subscription expires"""


class RequestorApi(ya_market.api.requestor_api.RequestorApi):
    """A replacement for market API that simulates early subscription expiration.

    A call to `collect_offers(sub_id)` will raise `ApiException` indicating
    subscription expiration when at least `SUBSCRIPTION_EXPIRATION_TIME`
    elapsed after the given subscription has been created.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subscriptions: Dict[str, float] = {}

    def subscribe_demand(self, demand, **kwargs):
        """Override `RequestorApi.subscribe_demand()` to register subscription create time."""
        id_coro = super().subscribe_demand(demand, **kwargs)

        async def coro():
            id = await id_coro
            self.subscriptions[id] = time.time()
            return id

        return coro()

    def collect_offers(self, subscription_id, **kwargs):
        """Override `RequestorApi.collect_offers()`.

        Raise `ApiException(404)` if at least `SUBSCRIPTION_EXPIRATION_TIME` elapsed
        since the subscription identified by `subscription_id` has been created.
        """
        if time.time() > self.subscriptions[subscription_id] + SUBSCRIPTION_EXPIRATION_TIME:
            logger.info("Subscription expired")

            async def coro():
                raise ApiException(
                    http_resp=Mock(
                        status=404,
                        reason="Not Found",
                        data=f"{{'message': 'Subscription [{subscription_id}] expired.'}}",
                    )
                )

            return coro()
        else:
            return super().collect_offers(subscription_id, **kwargs)


@pytest.fixture(autouse=True)
def patch_collect_offers(monkeypatch):
    """Install the patched `RequestorApi` class."""
    monkeypatch.setattr(yapapi.rest.market, "RequestorApi", RequestorApi)


async def unsubscribe_demand(sub_id: str) -> None:
    """Auxiliary function that calls `unsubscribeDemand` operation for given `sub_id`."""
    config = yapapi.rest.Configuration()
    market_client = config.market()
    requestor_api = yapapi.rest.market.RequestorApi(market_client)
    await requestor_api.unsubscribe_demand(sub_id)


async def assert_demand_resubscribed(events: "EventStream[Event]"):
    """A temporal assertion that the requestor will have to satisfy."""

    subscription_ids: Set[str] = set()

    async def wait_for_event(event_type: Type[Event], timeout: float):
        e = await eventually(events, lambda e: isinstance(e, event_type), timeout)
        assert e, f"Timed out waiting for {event_type}"
        logger.info(colors.cyan(str(e)))
        return e

    e = await wait_for_event(JobStarted, 10)

    # Make sure new subscriptions are created at least three times
    while len(subscription_ids) < 3:
        e = await wait_for_event(SubscriptionCreated, SUBSCRIPTION_EXPIRATION_TIME + 10)
        assert e.subscription.id not in subscription_ids
        subscription_ids.add(e.subscription.id)

    # Unsubscribe and make sure new subscription is created
    await unsubscribe_demand(e.subscription.id)
    logger.info("Demand unsubscribed")
    await wait_for_event(SubscriptionCreated, 5)

    # Enough checking, wait until the computation finishes
    await wait_for_event(JobFinished, 20)


@pytest.mark.asyncio
async def test_demand_resubscription(log_dir: Path, goth_config_path: Path, monkeypatch, config_overrides: List[Override]) -> None:
    """Test that checks that a demand is re-submitted after its previous submission expires."""

    configure_logging(log_dir)

    # Override the default test configuration to create only one provider node
    nodes = [
        {"name": "requestor", "type": "Requestor"},
        {"name": "provider-1", "type": "VM-Wasm-Provider", "use-proxy": True},
    ]
    goth_config = load_yaml(goth_config_path, config_overrides + [("nodes", nodes)])

    vm_package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]
        env = dict(os.environ)
        env.update(requestor.get_agent_env_vars())

        # Setup the environment for the requestor
        for key, val in env.items():
            monkeypatch.setenv(key, val)

        monitor = EventMonitor()
        monitor.add_assertion(assert_demand_resubscribed)
        monitor.start()

        # The requestor

        enable_default_logger()

        async def worker(work_ctx, tasks):
            async for task in tasks:
                script = work_ctx.new_script()
                script.run("/bin/sleep", "5")
                yield script
                task.accept_result()

        async with Golem(
            budget=10.0,
            event_consumer=monitor.add_event_sync,
        ) as golem:

            task: Task  # mypy needs this for some reason
            async for task in golem.execute_tasks(
                worker,
                [Task(data=n) for n in range(20)],
                vm_package,
                max_workers=1,
                timeout=timedelta(seconds=30),
            ):
                logger.info("Task %d computed", task.data)

        await monitor.stop()
        for a in monitor.failed:
            raise a.result()
