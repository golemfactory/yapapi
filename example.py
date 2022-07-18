import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode

allocation_id = "dbd84126-2836-4c8c-a265-274b47b0d744"
demand_id = "cd5e4cadaac74378bcc2f980c364faea-ba9d7b574660d17d7bc31ac35fc6cd18a62191af8a538b711e49c4b7fdc02e8d"
offer_id = "R-45fecc117f64a8d0e307d875977e4f3ebd6e2bd6aad75642fc19aeab59e40ac5"
image_hash = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"

golem = GolemNode()
print(golem)


async def example_1(golem):
    """Show existing allocation/demand"""
    allocation = golem.allocation(allocation_id)
    demand = golem.demand(demand_id)
    offer = golem.offer(offer_id)

    print(allocation)
    print(demand)
    print(offer)

    async with golem:
        await allocation.load()
        await demand.load()
        await offer.load(demand_id)

    print(allocation.data)
    print(demand.data)
    print(offer.data)

    #   All objects are singletons
    assert allocation == golem.allocation(allocation_id)
    assert demand == golem.demand(demand_id)
    assert offer == golem.offer(offer_id)


async def example_2(golem):
    """Show all current allocations/demands"""
    async with golem:
        for allocation in await golem.allocations():
            print(allocation)

        for demand in await golem.demands():
            print(demand)


async def example_3(golem):
    """Create new allocation, demand, fetch a single offer, cleanup"""
    async with golem:
        allocation = await golem.create_allocation(1)
        print(allocation)

        payload = await vm.repo(image_hash=image_hash)
        demand = await golem.create_demand(payload, allocations=[allocation])
        print(demand)

        async for offer in demand.offers():
            print(offer)
            break

        #   NOTE: these are redundant because both demand and allocation were
        #         created in autoclose=True mode
        await demand.unsubscribe()
        await allocation.release()


async def example_4(golem):
    """EventBus usage"""
    from yapapi.mid import events
    from yapapi.mid.market import Offer

    async def event_emitter(event) -> None:
        print("GOT EVENT", event)

    event_bus = golem.event_bus
    event_bus.listen(event_emitter)
    async with golem:
        event_bus.emit(events.ResourceCreated(Offer(golem, 'aaa')))


async def main():
    # print("\n---------- EXAMPLE 1 -------------\n")
    # await example_1(golem)
    print("\n---------- EXAMPLE 2 -------------\n")
    await example_2(golem)
    print("\n---------- EXAMPLE 3 -------------\n")
    await example_3(golem)
    print("\n---------- EXAMPLE 4 -------------\n")
    await example_4(golem)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
