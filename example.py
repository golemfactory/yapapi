import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode

allocation_id = "76a130ff-a334-4d17-9e92-14edf4dc0d78"
demand_id = "21fd7ad974fd4451b855674cf197f552-0cc8747670fab54e120c063087a72114b08e8d60eafb6993158436ecbea1a43b"
image_hash = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"

golem = GolemNode()
print(golem)


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
        demand = await golem.create_demand(payload, [allocation])
        print(demand)

        async for offer in demand.offers():
            print(offer)
            break

        await allocation.delete()
        await demand.delete()


async def main():
    await example_1(golem)
    await example_2(golem)
    await example_3(golem)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
