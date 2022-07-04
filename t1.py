import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode

# conf = [
#     ('allocation', '0f04a294-5495-4084-93c7-65c11e05e873'),
#     ('demand', '8e79ea564857484a99a3976b77bffb4c-c8a1f15e8b0c905474b57cf64b7316c0fe4ee5412c455ae7ad8d7cda89b18597'),
#     ('offer', 'R-eb82848f412e880b220057ac2feac0eec5d97f0455912c24cff373012e19422a', '8e79ea564857484a99a3976b77bffb4c-c8a1f15e8b0c905474b57cf64b7316c0fe4ee5412c455ae7ad8d7cda89b18597'),
#     ('agreement', '348e55cd4d6dc7bb89c5745142ec748c00e8d73f2da84228a12ba16be34517a1'),
# ]
# objects = []
# for name, id_, *args in conf:
#     objects.append([getattr(golem, name)(id_), *args])
# print(objects)

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
        demand = await golem.create_demand(payload)
        print("CREATED DEMAND", demand)
        await demand.load()
        print(demand.data)


async def main():
    await test_demand_create()
    await test_allocation_create()
    await test_collections()
    await test_delete()


if __name__ == '__main__':
    asyncio.run(main())
