"""Test if subscription expiration is handled correctly by Executor"""
from datetime import timedelta
import logging
import os
from pathlib import Path
import time
from typing import Dict, Set
from unittest.mock import Mock

import colors
import pytest

from goth.assertions import EventStream
from goth.assertions.monitor import EventMonitor
from goth.assertions.operators import eventually

from goth.configuration import load_yaml
from goth.runner import Runner
from goth.runner.log import configure_logging, monitored_logger
from goth.runner.probe import RequestorProbe

from yapapi import Executor, Task
from yapapi.executor.events import (
    Event,
    ComputationStarted,
    ComputationFinished,
    SubscriptionCreated,
)
import yapapi.rest.market
from yapapi.log import enable_default_logger
from yapapi.package import vm

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


@pytest.mark.asyncio
async def test_resubmission(log_dir: Path, monkeypatch) -> None:
    """Test that a demand is re-submitted after the previous submission expires."""

    configure_logging(log_dir)

    goth_config = load_yaml(Path(__file__).parent / "assets" / "goth-config.yml")

    vm_package = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    containers = [
        container for container in goth_config.containers if container.name != "provider-2"
    ]

    async def assertion(events: "EventStream[Event]"):
        """A temporal assertion that the requestor has to satisfy.

        It states that a `SubscriptionCreated` event occurs at least
        three times during requestor execution.
        """
        subscription_ids: Set[str] = set()

        e = await eventually(events, lambda e: isinstance(e, ComputationStarted), 10)
        assert e, "Timed out waiting for ComputationStarted"
        logger.info(colors.cyan(str(e)))

        while len(subscription_ids) < 3:
            e = await eventually(events, lambda e: isinstance(e, SubscriptionCreated), 15)
            assert isinstance(e, SubscriptionCreated), "Timed out waiting for SubscriptionCreated"
            logger.info(colors.cyan(str(e)))
            assert e.sub_id not in subscription_ids
            subscription_ids.add(e.sub_id)

        e = await eventually(events, lambda e: isinstance(e, ComputationFinished), 20)
        assert e, "Timed out waiting for ComputationFailed"
        logger.info(colors.cyan(str(e)))

    async with runner(containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]
        env = {**os.environ}
        requestor.set_agent_env_vars(env)

        # Setup the environment for the requestor
        for key, val in env.items():
            monkeypatch.setenv(key, val)

        monitor = EventMonitor()
        monitor.add_assertion(assertion)
        monitor.start()

        # The requestor

        enable_default_logger()

        async def worker(work_ctx, tasks):
            async for task in tasks:
                work_ctx.run("/bin/sleep", "5")
                yield work_ctx.commit()
                task.accept_result()

        async with Executor(
            budget=10.0,
            package=vm_package,
            max_workers=1,
            timeout=timedelta(seconds=25),
            event_consumer=monitor.add_event_sync,
        ) as executor:

            task: Task  # mypy needs this for some reason
            async for task in executor.submit(worker, [Task(data=n) for n in range(20)]):
                logger.info("Task %d computed", task.data)

        await monitor.stop()
        for a in monitor.failed:
            raise a.result()
