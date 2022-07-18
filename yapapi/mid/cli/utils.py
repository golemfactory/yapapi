import asyncio
from dataclasses import dataclass, MISSING
from typing import Callable, List
from functools import wraps

from yapapi.payload import Payload
from yapapi.props.base import constraint
from yapapi.props import inf

from yapapi.mid.payment import Allocation
from yapapi.mid.market import Demand
from yapapi.mid.golem_node import GolemNode


def _format_allocations(allocations: List[Allocation]) -> str:
    return "\n".join(["Allocations"] + [a.id for a in allocations])


def _format_demands(demands: List[Demand]) -> str:
    return "\n".join(["Demands"] + [d.id for d in demands])


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
