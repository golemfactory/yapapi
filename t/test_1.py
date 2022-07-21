import pytest
from random import random

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode
from yapapi.mid.exceptions import NoMatchingAccount, ResourceNotFound


@pytest.fixture
async def golem():
    try:
        yield GolemNode()
    finally:
        #   Cleanup
        async with GolemNode() as golem:
            for demand in await golem.demands():
                await demand.unsubscribe()
            for allocation in await golem.allocations():
                await allocation.release()


@pytest.fixture
async def any_payload():
    image_hash = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"
    return await vm.repo(image_hash=image_hash)


@pytest.mark.asyncio
async def test_singletons(golem):
    async with golem:
        assert golem.allocation('foo') is golem.allocation('foo')
        assert golem.demand('foo') is golem.demand('foo')
        assert golem.offer('foo', 'bar') is golem.offer('foo', 'bar') is golem.demand('bar').offer('foo')

        allocation = await golem.create_allocation(1)
        assert allocation is golem.allocation(allocation.id)


@pytest.mark.asyncio
async def test_allocation(golem):
    async with golem:
        amount = random()
        allocation = await golem.create_allocation(amount=amount)
        assert float(allocation.data.total_amount) == amount

        old_data = allocation.data
        await allocation.get_data()
        assert allocation.data == old_data

        await allocation.release()
        with pytest.raises(ResourceNotFound):
            await allocation.get_data(force=True)

        with pytest.raises(NoMatchingAccount):
            await golem.create_allocation(1, 'no_such_network_oops')


@pytest.mark.asyncio
async def test_demand(any_payload, golem):
    async with golem:
        allocation = await golem.create_allocation(1)
        demand = await golem.create_demand(any_payload, allocations=[allocation])

        async for offer in demand.initial_offers():
            break

        await demand.get_data()

        await demand.unsubscribe()
        with pytest.raises(ResourceNotFound):
            await demand.get_data(force=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("autoclose", (True, False))
async def test_autoclose(any_payload, golem, autoclose):
    async with golem:
        allocation = await golem.create_allocation(1, autoclose=autoclose)
        demand = await golem.create_demand(any_payload, allocations=[allocation], autoclose=autoclose)

    async with golem:
        if autoclose:
            with pytest.raises(ResourceNotFound):
                await demand.get_data(force=True)
            with pytest.raises(ResourceNotFound):
                await allocation.get_data(force=True)
        else:
            await demand.get_data(force=True)
            await demand.unsubscribe()
            await allocation.get_data(force=True)
            await allocation.release()
