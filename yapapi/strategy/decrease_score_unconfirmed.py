import logging

from typing import Set
from yapapi import events
from yapapi.props.builder import DemandBuilder
from yapapi import rest

from .base import MarketStrategy


class DecreaseScoreForUnconfirmedAgreement(MarketStrategy):
    """A market strategy that modifies a base strategy based on history of agreements."""

    base_strategy: MarketStrategy
    factor: float

    def __init__(self, base_strategy, factor):
        """
        :param base_strategy: the base strategy around which this strategy is wrapped
        :param factor: the factor by which the score of an offer for a provider which
                       failed to confirm the last agreement proposed to them will be multiplied
        """
        self.base_strategy = base_strategy
        self.factor = factor
        self._logger = logging.getLogger(f"{__name__}.{type(self).__name__}")
        self._rejecting_providers: Set[str] = set()

    def on_event(self, event: events.Event) -> None:
        if isinstance(event, events.AgreementConfirmed):
            self._rejecting_providers.discard(event.provider_id)
        elif isinstance(event, events.AgreementRejected):
            self._rejecting_providers.add(event.provider_id)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Decorate `demand` using the base strategy."""
        await self.base_strategy.decorate_demand(demand)

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer` using the base strategy and apply penalty if needed.

        If the offer issuer failed to approve the previous agreement (if any)
        and the base score is positive, then the base score is multiplied by `self.factor`.
        """
        score = await self.base_strategy.score_offer(offer)
        if offer.issuer in self._rejecting_providers and score > 0:
            self._logger.debug("Decreasing score for offer %s from '%s'", offer.id, offer.issuer)
            score *= self.factor
        return score
