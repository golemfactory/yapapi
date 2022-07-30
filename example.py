import asyncio

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode
from yapapi.mid.events import ResourceEvent

IMAGE_HASH = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"


async def example_1(allocation_id: str, demand_id: str, proposal_id: str) -> None:
    """Show existing allocation/demand/proposal"""
    golem = GolemNode()

    allocation = golem.allocation(allocation_id)
    demand = golem.demand(demand_id)
    proposal = golem.proposal(proposal_id, demand_id)

    print(allocation)
    print(demand)
    print(proposal)

    async with golem:
        await allocation.get_data()
        await demand.get_data()
        await proposal.get_data()

    print(allocation.data)
    print(demand.data)
    print(proposal.data)

    #   All objects are singletons
    assert allocation is golem.allocation(allocation_id)
    assert demand is golem.demand(demand_id)
    assert proposal is golem.proposal(proposal_id, demand_id)


async def example_2() -> None:
    """Show all current allocations/demands"""
    golem = GolemNode()
    async with golem:
        for allocation in await golem.allocations():
            print(allocation)

        for demand in await golem.demands():
            print(demand)


async def example_3() -> None:
    """Create new allocation, demand, fetch a single proposal, cleanup"""
    golem = GolemNode()
    async with golem:
        allocation = await golem.create_allocation(1)
        print(allocation)

        payload = await vm.repo(image_hash=IMAGE_HASH)
        demand = await golem.create_demand(payload, allocations=[allocation])
        print(demand)

        async for proposal in demand.initial_proposals():
            print(proposal)
            break

        #   NOTE: these are redundant because both demand and allocation were
        #         created in autoclose=True mode
        await demand.unsubscribe()
        await allocation.release()


async def example_4() -> None:
    """Respond to a proposal. Receive a conuterproposal. Reject it."""
    golem = GolemNode()
    async with golem:
        allocation = await golem.create_allocation(1)
        payload = await vm.repo(image_hash=IMAGE_HASH)
        demand = await golem.create_demand(payload, allocations=[allocation])

        #   Respond to proposals until we get a counterproposal
        async for proposal in demand.initial_proposals():
            our_response = await proposal.respond()
            print(f"We responded to {proposal} with {our_response}")
            try:
                their_response = await our_response.responses().__anext__()
                print(f"... and they responded with {their_response}")
                break
            except StopAsyncIteration:
                print("... and they rejected it")
                await our_response.get_data(force=True)
                assert our_response.data.state == "Rejected"

        #   Reject their counterproposal
        await their_response.reject()
        await their_response.get_data(force=True)
        assert their_response.data.state == "Rejected"
        print(f"... and we rejected it")

        #   The proposal tree
        assert their_response.parent is our_response
        assert our_response.parent is proposal
        assert proposal.parent is demand


async def example_5() -> None:
    """EventBus usage example"""
    golem = GolemNode()
    got_events = []

    async def on_event(event: ResourceEvent) -> None:
        got_events.append(event)

    golem.event_bus.resource_listen(on_event)
    async with golem:
        allocation = await golem.create_allocation(1)

    assert len(got_events) == 2
    assert got_events[0].resource == allocation
    assert got_events[1].resource == allocation

    from yapapi.mid import events
    assert isinstance(got_events[0], events.NewResource)
    assert isinstance(got_events[1], events.ResourceClosed)


async def main() -> None:
    # NOTE: this example assumes correct allocation/demand/proposal IDs
    # print("\n---------- EXAMPLE 1 -------------\n")
    # allocation_id = "94a0b27b-6f35-4dc4-98e2-6538bffe3c09"
    # demand_id = "07a5a0aa1c0641e3995b0afc4c9d0a63-9d135ea900e850d273f459a7ea218f5b489b4a0ec9789f059cb470780505efc8"
    # proposal_id = "R-83e5d06932340fddc1ba9b913c00621fac8b4628e2601dc9ab68df97435dd8df"
    # await example_1(allocation_id, demand_id, proposal_id)

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
