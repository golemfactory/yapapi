import asyncio
from dataclasses import dataclass, MISSING
from datetime import datetime
from typing import Any, Awaitable, Callable, List, TypeVar
from functools import wraps
import re

from prettytable import PrettyTable

from yapapi.payload import Payload
from yapapi.props.base import constraint
from yapapi.props import inf

from yapapi.mid.payment import Allocation
from yapapi.mid.market import Demand
from yapapi.mid.golem_node import GolemNode


def format_allocations(allocations: List[Allocation]) -> str:
    x = PrettyTable()
    x.field_names = ["id", "address", "network", "driver", "total", "remaining", "timeout"]
    for allocation in allocations:
        data = allocation.data
        assert data.payment_platform is not None  # mypy
        network, driver, _ = data.payment_platform.split('-')
        x.add_row([
            allocation.id,
            data.address,
            network,
            driver,
            data.total_amount,
            data.remaining_amount,
            data.timeout.isoformat(" ", "seconds") if data.timeout is not None else '',
        ])

    return x.get_string()  # type: ignore


def format_demands(demands: List[Demand]) -> str:
    x = PrettyTable()
    x.field_names = ["id", "subnet", "created"]
    for demand in demands:
        data = demand.data
        subnet = data.properties['golem.node.debug.subnet']

        #   According to ya_client spec, this should be ya_market.models.Timestamp, but is datetime
        #   Maybe this is a TODO for ya_client?
        timestamp: datetime = data.timestamp  # type: ignore

        created = timestamp.isoformat(" ", "seconds")
        x.add_row([
            demand.id,
            subnet,
            created,
        ])
    return x.get_string()  # type: ignore


@dataclass
class CliPayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=MISSING)


def parse_timedelta_str(timedelta_str: str) -> float:
    """Accepted formats: [float_or_int][|s|m|h|d]"""
    #   TODO: make this compatible with some standard (e.g. "123sec" should be ok maybe?)

    regexp = r'^(\d+|\d+\.\d+)([smhd])?$'
    match = re.search(regexp, timedelta_str)
    if not match:
        raise ValueError(f"timeout doesn't match the expected format")

    num = float(match.groups()[0])
    what = match.groups()[1] or "s"

    as_seconds = {"s": 1, "m": 60, "h": 60 * 60, "d": 60 * 60 * 24}
    return num * as_seconds[what]


R = TypeVar("R")


def async_golem_wrapper(f: Callable[..., Awaitable[R]]) -> Callable[..., R]:
    """Wraps an async function. Returns a sync function that:

    * starts a GolemNode and passes it as the first argument
    * executes the coroutine in a loop
    """
    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        async def with_golem_node() -> R:
            async with GolemNode() as golem:
                return await f(golem, *args, **kwargs)

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(with_golem_node())
    return wrapper
