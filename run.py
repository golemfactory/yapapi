import asyncio
from datetime import timedelta
from random import random

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode
from yapapi.mid.market import Proposal

from yapapi.mid.chain import Chain, SimpleScorer, DefaultNegotiator, AgreementCreator
from yapapi.mid.default_logger import DefaultLogger


IMAGE_HASH = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"


async def score_proposal(proposal: Proposal):
    return random()


async def max_3(resources):
    #   This function can be inserted anywhere in the example chain
    #   (except as the first element)
    cnt = 0
    async for resource in resources:
        yield resource
        cnt += 1
        if cnt == 3:
            break


async def main():
    golem = GolemNode()
    golem.event_bus.listen(DefaultLogger().on_event)

    async with golem:
        allocation = await golem.create_allocation(1)
        payload = await vm.repo(image_hash=IMAGE_HASH)
        demand = await golem.create_demand(payload, allocations=[allocation])

        chain = Chain(
            demand.initial_proposals(),
            SimpleScorer(score_proposal, min_proposals=10, max_wait=timedelta(seconds=1)),
            DefaultNegotiator(buffer_size=5),
            AgreementCreator(),
            max_3,
        )
        async for agreement in chain:
            print(f"--> {agreement}")

            #   This stops the demand.initial_proposals() generator
            #   and thus (finally, after all current proposals are processed) whole chain
            demand.set_no_more_children()

            #   Low-level objects form a tree - each has a parent and children.
            #   E.g. agreement.parent is the final proposal, and agreement.parent.parent
            #   is our last response that lead us to the final proposal.
            #   Proposal state is not auto-updated, but we can update it manually:
            assert agreement.parent.data.state == "Draft"
            await agreement.parent.get_data(force=True)
            assert agreement.parent.data.state == "Accepted"


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
