import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode

golem = GolemNode()
print(golem)


async def test_collections():
    '''Use all collection-returning methods, ensure additional load() changes nothing'''
    names = ['allocation', 'demand']
    async with golem:
        for name in names:
            fname = name + 's'
            print(f"Getting {name}s via GolemNode.{fname}()")
            objects = await getattr(golem, fname)()
            for obj in objects:
                print('  ', obj)
                old_data = obj.data.to_dict()
                await obj.load()
                assert old_data == obj.data.to_dict()


async def test_delete():
    names = ['demand', 'allocation']
    async with golem:
        for name in names:
            fname = name + 's'
            func = getattr(golem, fname)
            objects = await func()
            for obj in objects:
                print("DELETE", obj)
                await obj.delete()
            assert not (await func())


async def test_allocation_create():
    async with golem:
        allocation = await golem.create_allocation(amount=1)
        print("CREATED ALLOCATION", allocation)
        print(allocation.data)


async def test_demand_create():
    payload = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
    )
    async with golem:
        for allocations in ((), (await golem.create_allocation(amount=2),)):
            demand = await golem.create_demand(payload, allocations)
            print("CREATED DEMAND", demand)
            await demand.load()
            print(demand.data)


async def test_get_offers():
    payload = await vm.repo(
        image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
    )
    async with golem:
        allocation = await golem.create_allocation(amount=2)
        demand = await golem.create_demand(payload, [allocation])
        await demand.load()

        i = 0
        async for offer in demand.offers():
            print("GOT OFFER", offer)
            # print(offer.data)

            i += 1
            if i > 7:
                break


async def main():
    await test_get_offers()
    await test_demand_create()
    await test_allocation_create()
    await test_collections()
    await test_delete()


if __name__ == '__main__':
    asyncio.run(main())
