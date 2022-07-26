import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode

IMAGE_HASH = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"


async def example_1(allocation_id, demand_id, offer_id):
    """Show existing allocation/demand"""
    golem = GolemNode()
    allocation = golem.allocation(allocation_id)
    demand = golem.demand(demand_id)
    offer = golem.offer(offer_id, demand_id)

    print(allocation)
    print(demand)
    print(offer)

    async with golem:
        await allocation.get_data()
        await demand.get_data()
        await offer.get_data()

    print(allocation.data)
    print(demand.data)
    print(offer.data)

    #   All objects are singletons
    assert allocation is golem.allocation(allocation_id)
    assert demand is golem.demand(demand_id)
    assert offer is golem.offer(offer_id, demand_id)


async def example_2():
    golem = GolemNode()
    """Show all current allocations/demands"""
    async with golem:
        for allocation in await golem.allocations():
            print(allocation)

        # for demand in await golem.demands():
        #     print(demand)


async def example_3():
    """Create new allocation, demand, fetch a single offer, cleanup"""
    golem = GolemNode()
    async with golem:
        allocation = await golem.create_allocation(1)
        print(allocation)

        payload = await vm.repo(image_hash=IMAGE_HASH)
        demand = await golem.create_demand(payload, allocations=[allocation])
        print(demand)

        async for offer in demand.initial_offers():
            print(offer)
            break

        #   NOTE: these are redundant because both demand and allocation were
        #         created in autoclose=True mode
        await demand.unsubscribe()
        await allocation.release()


async def example_4():
    """Respond to an offer. Receive a conuteroffer. Reject it."""
    golem = GolemNode()
    async with golem:
        allocation = await golem.create_allocation(1)
        payload = await vm.repo(image_hash=IMAGE_HASH)
        demand = await golem.create_demand(payload, allocations=[allocation])

        #   Respond to offers until we get a counteroffer
        async for offer in demand.initial_offers():
            our_response = await offer.respond()
            print(f"We responded to {offer} with {our_response}")
            try:
                their_response = await our_response.responses().__anext__()
                print(f"... and they responded with {their_response}")
                break
            except StopAsyncIteration:
                print("... and they rejected it")
                await our_response.get_data(force=True)
                assert our_response.data.state == "Rejected"

        #   Reject their counteroffer
        await their_response.reject()
        await their_response.get_data(force=True)
        assert their_response.data.state == "Rejected"
        print(f"... and we rejected it")

        #   The offer tree
        assert their_response.parent is our_response
        assert our_response.parent is offer
        assert offer.parent is demand


async def example_5():
    """EventBus usage"""
    golem = GolemNode()
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
    # allocation_id = "715e5db0-472e-4b93-a286-88e2015c1a2e"
    # demand_id = "87cb58c918b4480fb13a1089e275cbde-1af4ea079292d70e9ab15b786098e70e01313a044903c4d4ec9deb6b28db9a20"
    # offer_id = "R-786e99dbc162c910d904d882e700380ee1a51b946485eb3a9096095b56414e68"
    # await example_1(allocation_id, demand_id, offer_id)

    print("\n---------- EXAMPLE 2 -------------\n")
    await example_2()

    print("\n---------- EXAMPLE 3 -------------\n")
    await example_3()

    print("\n---------- EXAMPLE 4 -------------\n")
    await example_4()

    print("\n---------- EXAMPLE 5 -------------\n")
    await example_5()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
