import asyncio
from collections import namedtuple

from yapapi.payload import vm

from yapapi.mid.golem_node import GolemNode
from yapapi.mid.market import Offer

from yapapi.mid.offer_scorer import SimpleScorer


image_hash = "9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae"
golem = GolemNode()


async def score_offer(offer: Offer) -> float:
    #   Purpose: interface compatible with yapapi.rest.market.OfferProposal
    #            (enough to use some "old" strategy)
    from yapapi.strategy import LeastExpensiveLinearPayuMS
    CompatOffer = namedtuple('CompatOffer', ['id', 'props'])
    compat_offer = CompatOffer(offer.id, offer.data.properties)

    strategy = LeastExpensiveLinearPayuMS()
    return await strategy.score_offer(compat_offer)


async def main():
    """Create new allocation, demand, fetch a single offer, cleanup"""
    async with golem:
        allocation = await golem.create_allocation(1)
        payload = await vm.repo(image_hash=image_hash)
        demand = await golem.create_demand(payload, subnet='public-beta')

        simple_scorer = SimpleScorer(demand.offers(), score_offer, min_offers=100)
        offers = simple_scorer.offers()
        while True:
            offer, score = await offers.__anext__()
            print(score)
            await asyncio.sleep(1)

        await SimpleScorer.aclose()


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
