from yapapi.strategy import MarketStrategy, SCORE_REJECTED
from yapapi.props.builder import DemandBuilder
from yapapi.rest.market import OfferProposal
from typing import Iterable


class Blacklist(MarketStrategy):
    """Blacklist - extend a market strategy with a provider blacklist

    :param base_strategy: a market strategy that will be used to score offers from non-blacklisted providers
    :param blacklist: a collection of ids (`offer.provider_id`) of blacklisted providers. The collection can be updated
        after the initialization of the :class:`Blacklist`.

    Example 1. Add a blacklist to an arbitrary strategy::

        strategy = SomeStrategy(...)
        bad_providers = ['bad_provider_1', 'bad_provider_2']
        strategy = Blacklist(strategy, bad_providers)

    Example 2. Add a blacklist to the current strategy of a Golem instance::

        async with Golem(...) as golem:
            # ... whatever - this can be done when computations are already in progress
            golem.strategy = Blacklist(golem.strategy, ['bad_provider_1', 'bad_provider_2'])

    Example 3. Blacklist every provider that fails to create an activity::

        from yapapi import events

        blacklist = set()

        def blacklisting_event_consumer(event: events.Event):
            if isinstance(event, events.ActivityCreateFailed):
                blacklist.add(event.provider_id)

        async with Golem(...) as golem:
            await golem.add_event_consumer(blacklisting_event_consumer)
            golem.strategy = Blacklist(golem.strategy, blacklist)
    """
    def __init__(self, base_strategy: MarketStrategy, blacklist: Iterable[str]):
        self.base_strategy = base_strategy
        self.blacklist = blacklist

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        await self.base_strategy.decorate_demand(demand)

    async def score_offer(self, offer: OfferProposal, _todo_remove_this) -> float:
        if offer.issuer in self.blacklist:
            return SCORE_REJECTED
        return await self.base_strategy.score_offer(offer)
