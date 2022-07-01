import asyncio

from yapapi.mid.golem_node import GolemNode


allocation_id = '2136867a-6c6c-4784-879d-d3ed76646fdb'

golem = GolemNode()
allocation = golem.allocation(allocation_id)


async def main():
    print(allocation.data)
    async with golem:
        await allocation.load()
    print(allocation.data)


if __name__ == '__main__':
    asyncio.run(main())
