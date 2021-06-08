"""Implementation of strategies for choosing offers from market."""

import abc
from collections import defaultdict
from decimal import Decimal
import logging
from types import MappingProxyType
from typing import Dict, Mapping, Optional

from dataclasses import dataclass, field
from typing_extensions import Final, Protocol

from yapapi.props import com, Activity
from yapapi.props.builder import DemandBuilder, DemandDecorator
from yapapi import rest


SCORE_NEUTRAL: Final[float] = 0.0
SCORE_REJECTED: Final[float] = -1.0
SCORE_TRUSTED: Final[float] = 100.0


class ComputationHistory(Protocol):
    """A protocol for objects that provide information about the history of current computation."""

    def rejected_last_agreement(self, issuer_id) -> bool:
        """Return True iff the previous agreement proposed to `issuer_id` has been rejected."""
        ...


class MarketStrategy(DemandDecorator, abc.ABC):
    """Abstract market strategy."""

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Optionally add relevant constraints to a Demand."""

    async def score_offer(
        self, offer: rest.market.OfferProposal, history: Optional[ComputationHistory] = None
    ) -> float:
        """Score `offer`. Better offers should get higher scores."""
        return SCORE_REJECTED


@dataclass
class DummyMS(MarketStrategy, object):
    """A default market strategy implementation.

    Its `score_offer()` method returns `SCORE_NEUTRAL` for every offer with prices
    that do not exceed maximum prices specified for each counter.
    For other offers, returns `SCORE_REJECTED`.
    """

    max_for_counter: Mapping[com.Counter, Decimal] = MappingProxyType(
        {com.Counter.TIME: Decimal("0.002"), com.Counter.CPU: Decimal("0.002") * 10}
    )
    max_fixed: Decimal = Decimal("0.05")
    _activity: Optional[Activity] = field(init=False, repr=False, default=None)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Ensure that the offer uses `PriceModel.LINEAR` price model."""
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")
        self._activity = Activity.from_properties(demand.properties)

    async def score_offer(
        self, offer: rest.market.OfferProposal, history: Optional[ComputationHistory] = None
    ) -> float:
        """Score `offer`. Returns either `SCORE_REJECTED` or `SCORE_NEUTRAL`."""

        linear: com.ComLinear = com.ComLinear.from_properties(offer.props)

        if linear.scheme != com.BillingScheme.PAYU:
            return SCORE_REJECTED

        if linear.fixed_price > self.max_fixed:
            return SCORE_REJECTED
        for counter, price in linear.price_for.items():
            if counter not in self.max_for_counter:
                return SCORE_REJECTED
            if price > self.max_for_counter[counter]:
                return SCORE_REJECTED

        return SCORE_NEUTRAL


@dataclass
class LeastExpensiveLinearPayuMS(MarketStrategy, object):
    """A strategy that scores offers according to cost for given computation time."""

    def __init__(
        self,
        expected_time_secs: int = 60,
        max_fixed_price: Decimal = Decimal("inf"),
        max_price_for: Mapping[com.Counter, Decimal] = MappingProxyType({}),
    ):
        self._expected_time_secs = expected_time_secs
        self._logger = logging.getLogger(f"{__name__}.{type(self).__name__}")
        self._max_fixed_price = max_fixed_price if max_fixed_price is not None else Decimal("inf")
        self._max_price_for: Dict[com.Counter, Decimal] = defaultdict(lambda: Decimal("inf"))
        self._max_price_for.update(max_price_for)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Ensure that the offer uses `PriceModel.LINEAR` price model."""
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")

    async def score_offer(
        self, offer: rest.market.OfferProposal, history: Optional[ComputationHistory] = None
    ) -> float:
        """Score `offer` according to cost for expected computation time."""

        linear: com.ComLinear = com.ComLinear.from_properties(offer.props)
        self._logger.debug("Scoring offer %s, parameters: %s", offer.id, linear)
        if linear.scheme != com.BillingScheme.PAYU:
            self._logger.debug(
                "Rejected offer %s: unsupported scheme '%s'", offer.id, linear.scheme
            )
            return SCORE_REJECTED

        known_time_prices = {com.Counter.TIME, com.Counter.CPU}

        for counter in linear.price_for.keys():
            if counter not in known_time_prices:
                self._logger.debug("Rejected offer %s: unsupported counter '%s'", offer.id, counter)
                return SCORE_REJECTED

        if linear.fixed_price > self._max_fixed_price:
            self._logger.debug(
                "Rejected offer %s: fixed price higher than fixed price cap %f.",
                offer.id,
                self._max_fixed_price,
            )
            return SCORE_REJECTED

        if linear.fixed_price < 0:
            self._logger.debug("Rejected offer %s: negative fixed price", offer.id)
            return SCORE_REJECTED
        expected_price = linear.fixed_price

        for resource in known_time_prices:

            if linear.price_for[resource] > self._max_price_for[resource]:
                self._logger.debug(
                    "Rejected offer %s: price for '%s' higher than price cap %f.",
                    offer.id,
                    resource,
                    self._max_price_for[resource],
                )
                return SCORE_REJECTED

            if linear.price_for[resource] < 0:
                self._logger.debug("Rejected offer %s: negative price for '%s'", offer.id, resource)
                return SCORE_REJECTED

            expected_price += linear.price_for[resource] * self._expected_time_secs

        # The higher the expected price value, the lower the score.
        # The score is always lower than SCORE_TRUSTED and is always higher than 0.
        score = SCORE_TRUSTED * 1.0 / (expected_price + 1.01)
        return score


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

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Decorate `demand` using the base strategy."""
        await self.base_strategy.decorate_demand(demand)

    async def score_offer(
        self, offer: rest.market.OfferProposal, history: Optional[ComputationHistory] = None
    ) -> float:
        """Score `offer` using the base strategy and apply penalty if needed.

        If the offer issuer failed to approve the previous agreement (if any)
        then the base score is multiplied by `self._factor`.
        """
        score = await self.base_strategy.score_offer(offer)
        if history and history.rejected_last_agreement(offer.issuer) and score > 0:
            self._logger.debug("Decreasing score for offer %s from '%s'", offer.id, offer.issuer)
            score *= self.factor
        return score
