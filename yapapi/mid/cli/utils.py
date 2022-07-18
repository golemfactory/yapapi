import asyncio
from dataclasses import dataclass, MISSING
from typing import Callable, List
from functools import wraps

from prettytable import PrettyTable

from yapapi.payload import Payload
from yapapi.props.base import constraint
from yapapi.props import inf

from yapapi.mid.payment import Allocation
from yapapi.mid.market import Demand
from yapapi.mid.golem_node import GolemNode


def _format_allocations(allocations: List[Allocation]) -> str:
    x = PrettyTable()
    x.field_names = ["id", "address", "network", "driver", "total", "remaining", "timeout"]
    for allocation in allocations:
        data = allocation.data
        network, driver, _ = data.payment_platform.split('-')
        x.add_row([
            allocation.id,
            data.address,
            network,
            driver,
            data.total_amount,
            data.remaining_amount,
            data.timeout.isoformat(" ", "seconds"),
        ])

    return x.get_string()


def _format_demands(demands: List[Demand]) -> str:
    x = PrettyTable()
    x.field_names = ["id", "subnet", "created"]
    for demand in demands:
        data = demand.data
        subnet = data.properties['golem.node.debug.subnet']
        created = data.timestamp.isoformat(" ", "seconds")
        x.add_row([
            demand.id,
            subnet,
            created,
        ])
    return x.get_string()


@dataclass
class _CliPayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=MISSING)


def async_golem_wrapper(f) -> Callable:
    @wraps(f)
    def wrapper(*args, **kwargs):
        async def with_golem():
            async with GolemNode() as golem:
                await f(golem, *args, **kwargs)

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(with_golem())
    return wrapper
