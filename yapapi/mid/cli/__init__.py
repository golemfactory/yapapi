import asyncio
from dataclasses import dataclass, MISSING
from functools import wraps

from typing import Tuple

import click

from yapapi.payload import Payload
from yapapi.props.base import constraint
from yapapi.props import inf

from yapapi.mid.golem_node import GolemNode
from yapapi.mid.payment import Allocation
from yapapi.mid.market import Demand


def _format_allocations(allocations: Tuple[Allocation]) -> str:
    return "\n".join(["Allocations"] + [a.id for a in allocations])


def _format_demands(demands: Tuple[Demand]) -> str:
    return "\n".join(["Demands"] + [d.id for d in demands])


@dataclass
class _CliPayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=MISSING)


def async_golem_wrapper(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        async def with_golem():
            async with GolemNode() as golem:
                await f(golem, *args, **kwargs)

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(with_golem())
    return wrapper


@click.group()
def cli():
    pass


@cli.group()
def allocation():
    pass


@allocation.command("list")
@async_golem_wrapper
async def allocation_list(golem):
    allocations = await golem.allocations()
    for allocation in allocations:
        click.echo(allocation)


@allocation.command("new")
@async_golem_wrapper
async def allocation_new(golem):
    #   TODO
    #   (waits for: does GolemNode know the network etc?)
    #   yapapi allocation new 50 --network polygon
    click.echo("ALLOCATION NEW")


@allocation.command("clean")
@async_golem_wrapper
async def allocation_clean(golem):
    for allocation in await golem.allocations():
        await allocation.release()
        click.echo(allocation.id)


@cli.command()
@async_golem_wrapper
async def status(golem):
    allocations = await golem.allocations()
    demands = await golem.demands()
    click.echo(_format_allocations(allocations))
    click.echo()
    click.echo(_format_demands(demands))


@cli.command()
@click.option(
    "--runtime",
    type=str,
    required=True,
)
@click.option(
    "--timeout",
    type=int,
    required=False,
)
@async_golem_wrapper
async def find_node(golem, runtime, timeout):
    #   TODO: subnet? etc?
    #   (waits for: does GolemNode know the network etc?)
    click.echo(f"Looking for offers for runtime {runtime} on the subnet {golem.subnet}")

    async def get_nodes():
        payload = _CliPayload(runtime)

        #   TODO: demand-as-contextmanager
        #   (waits for: interface?)
        demand = await golem.create_demand(payload)
        async for offer in demand.offers():
            click.echo(offer)

    try:
        await asyncio.wait_for(get_nodes(), timeout)
    except asyncio.TimeoutError:
        pass
