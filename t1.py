import asyncio

from yapapi.mid.golem_node import GolemNode

conf = [
    ('allocation', '0f04a294-5495-4084-93c7-65c11e05e873'),
    ('demand', '8e79ea564857484a99a3976b77bffb4c-c8a1f15e8b0c905474b57cf64b7316c0fe4ee5412c455ae7ad8d7cda89b18597'),
    ('offer', 'R-eb82848f412e880b220057ac2feac0eec5d97f0455912c24cff373012e19422a', '8e79ea564857484a99a3976b77bffb4c-c8a1f15e8b0c905474b57cf64b7316c0fe4ee5412c455ae7ad8d7cda89b18597'),
    ('agreement', '348e55cd4d6dc7bb89c5745142ec748c00e8d73f2da84228a12ba16be34517a1'),
]

golem = GolemNode()

objects = []
for name, id_, *args in conf:
    objects.append([getattr(golem, name)(id_), *args])
print(objects)


async def main():
    async with golem:
        for allocation in await golem.allocations():
            old_data = allocation.data.to_dict()
            await allocation.load()
            assert old_data == allocation.data.to_dict()
            print(allocation)
        # for obj, *args in objects:
        #     print("OBJ", obj)
        #     print("OBJ", obj)
        #     await obj.load(*args)
        #     print(obj.data)


if __name__ == '__main__':
    asyncio.run(main())
