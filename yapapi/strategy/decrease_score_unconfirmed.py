import logging
from typing import Set

from golem_core.core.market_api import DemandBuilder

from yapapi import events, rest

from .wrapping_strategy import WrappingMarketStrategy


class DecreaseScoreForUnconfirmedAgreement(WrappingMarketStrategy):
    """A market strategy wrapper that modifies scoring of providers based on history of agreements.

    It decreases the scores for providers that rejected our agreements.

    The strategy keeps an internal list of providers who have rejected proposed agreements and
    removes them from this list when they accept one.
    """

    factor: float

    def __init__(self, base_strategy, factor):
        """Initialize instance.

        :param base_strategy: the base strategy around which this strategy is wrapped
        :param factor: the factor by which the score of an offer for a provider which
                       failed to confirm the last agreement proposed to them will be multiplied
        """
        super().__init__(base_strategy)
        self.factor = factor
        self._logger = logging.getLogger(f"{__name__}.{type(self).__name__}")
        self._rejecting_providers: Set[str] = set()

    def on_event(self, event: events.Event) -> None:
        """Modify the internal `_rejecting_providers` list on `AgreementConfirmed/Rejected`.

        This method needs to be added as an event consumer in
         :func:`yapapi.Golem.add_event_consumer`.
        """
        if isinstance(event, events.AgreementConfirmed):
            self._rejecting_providers.discard(event.provider_id)
        elif isinstance(event, events.AgreementRejected):
            self._rejecting_providers.add(event.provider_id)

    async def decorate_demand_builder(self, demand: DemandBuilder) -> None:
        """Decorate `demand` using the base strategy."""
        await self.base_strategy.decorate_demand_builder(demand)

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
