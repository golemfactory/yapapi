import asyncio
from functools import wraps

import click

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode


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
        print(allocation)


@allocation.command("new")
@async_golem_wrapper
async def allocation_new(golem):
    #   TODO
    #   yapapi allocation new 50 --network polygon
    click.echo("ALLOCATION NEW")


@allocation.command("clean")
@async_golem_wrapper
async def allocation_clean(golem):
    for allocation in await golem.allocations():
        await allocation.release()
        print(allocation.id)


@cli.command()
@async_golem_wrapper
async def status(golem):
    #   TODO
    click.echo("STATUS")


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
    click.echo(f"Looking for offers for runtime {runtime}")

    async def get_nodes():
        #   TODO: better payload creation (image hash?)
        image_hash = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"
        payload = await vm.repo(image_hash=image_hash)

        #   TODO: demand-as-contextmanager
        demand = await golem.create_demand(payload)
        async for offer in demand.offers():
            print(offer)

    try:
        await asyncio.wait_for(get_nodes(), timeout)
    except asyncio.TimeoutError:
        pass
