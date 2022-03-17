"""
Provider Filter
^^^^^^^^^^^^^^^

Market strategy wrapper that enables easy exclusion of offers from certain providers using
a simple boolean condition, while preserving correct scoring of the remaining offers by the
base strategy.
"""
import inspect

from yapapi.strategy import BaseMarketStrategy, SCORE_REJECTED, WrappingMarketStrategy
from yapapi.rest.market import OfferProposal
from typing import Awaitable, Callable, Union

IsAllowedType = Union[
    Callable[[str], bool],
    Callable[[str], Awaitable[bool]],
]


class ProviderFilter(WrappingMarketStrategy):
    """ProviderFilter - extend a market strategy with a layer that excludes offers from certain issuers

    :param base_strategy: a market strategy that will be used to score offers from allowed providers
    :param is_allowed: a callable that accepts provider_id as an argument and returns either a boolean,
        or a boolean-returning awaitable, determining if offers from this provider should be considered
        (that is: scored by the `base_strategy`)

    Example 1. Block selected providers::

        bad_providers = ['bad_provider_1', 'bad_provider_2']
        base_strategy = SomeStrategy(...)
        strategy = ProviderFilter(base_strategy, lambda provider_id: provider_id not in bad_providers)

    Example 2. Select providers using a database table::

        #   create an async database connection
        #   (sync would also work, but could hurt `yapapi` overall performance)
        async_conn = ...

        async def is_allowed(provider_id):
            result = await async_conn.execute("SELECT 1 FROM allowed_providers WHERE provider_id = ?", provider_id)
            return bool(result.fetchall())

        base_strategy = SomeStrategy()
        strategy = ProviderFilter(base_strategy, is_allowed)

    Example 3. Use the default strategy, but disable every provider that fails to create an activity::

        from yapapi import events

        bad_providers = set()

        def denying_event_consumer(event: events.Event):
            if isinstance(event, events.ActivityCreateFailed):
                bad_providers.add(event.provider_id)

        golem = Golem(...)
        golem.strategy = ProviderFilter(golem.strategy, lambda provider_id: provider_id not in bad_providers)
        await golem.add_event_consumer(denying_event_consumer)

        async with golem:
            ...

        #   NOTE: this will currently work only for **new** offers from the provider, because old offers are already
        #   scored, this should improve in https://github.com/golemfactory/yapapi/issues/820
    """

    def __init__(self, base_strategy: BaseMarketStrategy, is_allowed: IsAllowedType):
        super().__init__(base_strategy)
        self._is_allowed = is_allowed

    async def score_offer(self, offer: OfferProposal) -> float:
        allowed = self._is_allowed(offer.issuer)
        if inspect.iscoroutinefunction(allowed):
            allowed = await allowed  # type: ignore

        if not allowed:
            return SCORE_REJECTED

        return await self.base_strategy.score_offer(offer)
