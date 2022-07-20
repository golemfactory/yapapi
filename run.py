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


async def get_offer(initial_offers):
    initial_offer = await initial_offers.__anext__()
    our_response = await initial_offer.respond()
    print(f"Responded with {our_response} to initial offer {initial_offer}")

    async for their_response in our_response.responses():
        print(f"They responded with {their_response} to {our_response}")
        return their_response

    await our_response.get_data()
    assert our_response.rejected

    print(f"Our response {our_response} was rejected")
    return None


async def main():
    """Create new allocation, demand, fetch a single offer, cleanup"""
    async with golem:
        allocation = await golem.create_allocation(1)
        payload = await vm.repo(image_hash=image_hash)
        demand = await golem.create_demand(payload, allocations=[allocation])

        simple_scorer = SimpleScorer(demand.initial_offers(), score_offer, min_offers=2)
        offers = simple_scorer.offers()

        while True:
            task = asyncio.create_task(get_offer(offers))
            try:
                proposal = await asyncio.wait_for(task, 10)
                if proposal is not None:
                    break
            except asyncio.TimeoutError:
                print("TIMEOUT")
        print("GOT PROPOSAL", proposal)
        await simple_scorer.aclose()

        agreement = await proposal.create_agreement()
        await agreement.confirm()


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
