import asyncio
from datetime import timedelta
from random import random

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode
from yapapi.mid.market import Offer

from yapapi.mid.chain import Chain, SimpleScorer, DefaultNegotiator, AgreementCreator


IMAGE_HASH = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"


async def score_offer(offer: Offer):
    return random()


async def max_3(resources):
    cnt = 0
    async for resource in resources:
        yield resource
        cnt += 1
        if cnt == 3:
            break


async def main():
    golem = GolemNode()
    async with golem:
        allocation = await golem.create_allocation(1)
        payload = await vm.repo(image_hash=IMAGE_HASH)
        demand = await golem.create_demand(payload, allocations=[allocation])

        chain = Chain(
            demand.initial_offers(),
            SimpleScorer(score_offer, min_offers=10, max_wait=timedelta(seconds=1)),
            DefaultNegotiator(buffer_size=5),
            AgreementCreator(),
        )
        async for agreement in chain:
            print(f"--> {agreement}")

            #   This stops the demand.initial_offers() generator
            #   and thus (finally, after all current offers are processed) whole chain
            demand.set_no_more_children()


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
