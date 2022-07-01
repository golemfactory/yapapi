import asyncio

from yapapi.mid.golem_node import GolemNode


allocation_id = '32db9173-87ed-4e47-84b8-7f4fbf78ebce'
demand_id = '5144990311f747118741619d0b1b5d98-f53acd52eb5ddc288763a4f150add508e0dd5960b9c7f11a7b1f7ad3526ee6de'
demand_id = 'fd68851603d44ddd8ebc276bea8f30a5-f9e4759598c733a49498448b86da8930274649573d3568600c3e65bf421d1094'
demand_id = '9a390c02b18a46169844d3fa3dffa3e1-5aed623d93a82ef036797d107c04ed9cec04f97af5dbfc9a244c8899df5829ed'
demand_id = '0e60c5fdde294ec4b8bc38508618fe7e-a141625ffc65fb64167f4467716c4441586af17ed3b3543b7ee64e481269b499'

golem = GolemNode()
allocation = golem.allocation(allocation_id)
demand = golem.demand(demand_id)


async def main():
    print(demand)
    async with golem:
        await allocation.load()
        await demand.load()
    print(demand.data)
    print(allocation.data)


if __name__ == '__main__':
    asyncio.run(main())
