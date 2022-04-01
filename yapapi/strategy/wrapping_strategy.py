import abc

from yapapi.props.builder import DemandBuilder
from yapapi import rest


from .base import BaseMarketStrategy


class WrappingMarketStrategy(BaseMarketStrategy, abc.ABC):
    """
    Helper abstract class which allows other/user defined strategies to wrap some other strategies,
    without overriding the attributes (e.g. defaults) defined on the derived-from strategy.

    WrappingMarketStrategy classes are unusable on their own and always have to wrap some base
    strategy.

    By default all attributes and method calls are forwarded to the `base_strategy`.
    """

    base_strategy: BaseMarketStrategy
    """base strategy wrapped by this wrapper."""

    def __init__(self, base_strategy: BaseMarketStrategy):
        """
        :param base_strategy: the base strategy around which this strategy is wrapped
        """
        self.base_strategy = base_strategy

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        await self.base_strategy.decorate_demand(demand)

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        return await self.base_strategy.score_offer(offer)

    async def respond_to_provider_offer(
        self,
        our_demand: DemandBuilder,
        provider_offer: rest.market.OfferProposal,
    ) -> DemandBuilder:
        return await self.base_strategy.respond_to_provider_offer(our_demand, provider_offer)

    def __getattr__(self, item):
        """Forward all calls for undefined properties and variables to the base class."""
        return getattr(self.base_strategy, item)
