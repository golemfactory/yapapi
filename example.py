import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode

allocation_id = "a5f7be7b-2890-4a5f-a93e-d109849de7fb"
demand_id = "32d198f1565f46aa917d704125b8e8ca-28e70eade4bffc91c3ab0b15b522878b47649169b27ae7659dffab4bb89ad0c0"
image_hash = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"


async def example_1(golem):
    """Show existing allocation/demand"""
    allocation = golem.allocation(allocation_id)
    demand = golem.demand(demand_id)

    print(allocation)
    print(demand)

    async with golem:
        await allocation.load()
        await demand.load()

    print(allocation.data)
    print(demand.data)

    #   All objects are singletons
    assert allocation == golem.allocation(allocation_id)
    assert demand == golem.demand(demand_id)


async def example_2(golem):
    """Show all current allocations/demands"""
    async with golem:
        for allocation in await golem.allocations():
            print(allocation)

        for demand in await golem.demands():
            print(demand)


async def example_3(golem):
    """Create new allocation, demand, fetch a single offer, cleanup.

    Also: this one works in non-contextmanager mode."""
    allocation = await golem.create_allocation(1)
    print(allocation)

    payload = await vm.repo(image_hash=image_hash)
    demand = await golem.create_demand(payload, [allocation])
    print(demand)

    async for offer in demand.offers():
        print(offer)
        break

    await allocation.release()
    await demand.unsubscribe()
    await golem.aclose()


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
    golem = GolemNode()
    print(golem)

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
