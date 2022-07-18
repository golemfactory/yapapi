import asyncio

import click

from yapapi.engine import DEFAULT_NETWORK, DEFAULT_DRIVER, DEFAULT_SUBNET

from yapapi.mid.golem_node import GolemNode
from .utils import _format_allocations, _format_demands, _CliPayload, async_golem_wrapper


@click.group()
def cli():
    pass


@cli.group()
def allocation():
    pass


@allocation.command("list")
@async_golem_wrapper
async def allocation_list(golem: GolemNode):
    allocations = await golem.allocations()
    click.echo(_format_allocations(allocations))


@allocation.command("new")
@click.argument("amount", type=float)
@click.option("--network", type=str, default=DEFAULT_NETWORK)
@click.option("--driver", type=str, default=DEFAULT_DRIVER)
@async_golem_wrapper
async def allocation_new(
    golem: GolemNode,
    amount: float,
    network: str,
    driver: str,
):
    allocation = await golem.create_allocation(
        amount,
        network=network,
        driver=driver,
        autoclose=False
    )
    click.echo(f"NEW ALLOCATION {allocation}")


@allocation.command("clean")
@async_golem_wrapper
async def allocation_clean(golem: GolemNode):
    for allocation in await golem.allocations():
        await allocation.release()
        click.echo(allocation.id)


@cli.command()
@async_golem_wrapper
async def status(golem: GolemNode):
    allocations = await golem.allocations()
    demands = await golem.demands()
    click.echo(_format_allocations(allocations))
    click.echo()
    click.echo(_format_demands(demands))


@cli.command()
@click.option("--runtime", type=str, required=True)
@click.option("--subnet", type=str, default=DEFAULT_SUBNET)
@click.option("--timeout", type=int, required=False)
@async_golem_wrapper
async def find_node(golem: GolemNode, runtime: str, subnet: str, timeout: int):
    click.echo(f"Looking for offers for runtime {runtime}")

    async def get_nodes():
        payload = _CliPayload(runtime)
        demand = await golem.create_demand(payload, subnet=subnet)
        async for offer in demand.offers():
            click.echo(offer)

    try:
        await asyncio.wait_for(get_nodes(), timeout)
    except asyncio.TimeoutError:
        pass
