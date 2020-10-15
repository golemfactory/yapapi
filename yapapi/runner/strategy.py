"""Implementation of strategies for choosing offers from market."""

import abc
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping, Optional

from dataclasses import dataclass, field
from typing_extensions import Final

from ..props import com, Activity
from ..props.builder import DemandBuilder
from .. import rest


SCORE_NEUTRAL: Final[float] = 0.0
SCORE_REJECTED: Final[float] = -1.0
SCORE_TRUSTED: Final[float] = 100.0

CFF_DEFAULT_PRICE_FOR_COUNTER: Final[Mapping[com.Counter, Decimal]] = MappingProxyType(
    {com.Counter.TIME: Decimal("0.002"), com.Counter.CPU: Decimal("0.002") * 10}
)


class MarketStrategy(abc.ABC):
    """Abstract market strategy."""

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Optionally add relevant constraints to a Demand."""

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer`. Better offers should get higher scores."""
        return SCORE_REJECTED


@dataclass
class DummyMS(MarketStrategy, object):
    """A default market strategy implementation.

    Its `score_offer()` method returns `SCORE_NEUTRAL` for every offer with prices
    that do not exceed maximum prices specified for each counter.
    For other offers, returns `SCORE_REJECTED`.
    """
    max_for_counter: Mapping[com.Counter, Decimal] = CFF_DEFAULT_PRICE_FOR_COUNTER
    max_fixed: Decimal = Decimal("0.05")
    _activity: Optional[Activity] = field(init=False, repr=False, default=None)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Ensure that the offer uses `PriceModel.LINEAR` price model."""
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")
        self._activity = Activity.from_props(demand.props)

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer`. Returns either `SCORE_REJECTED` or `SCORE_NEUTRAL`."""

        linear: com.ComLinear = com.ComLinear.from_props(offer.props)

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
    """A strategy that scores offers according to cost for a given computation time."""

    def __init__(self, expected_time_secs: int = 60):
        self._expected_time_secs = expected_time_secs

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Ensure that the offer uses `PriceModel.LINEAR` price model."""
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer` according to cost for expected computation time."""

        linear: com.ComLinear = com.ComLinear.from_props(offer.props)

        if linear.scheme != com.BillingScheme.PAYU:
            return SCORE_REJECTED

        known_time_prices = {com.Counter.TIME, com.Counter.CPU}

        for counter in linear.price_for.keys():
            if counter not in known_time_prices:
                return SCORE_REJECTED

        if linear.fixed_price < 0:
            return SCORE_REJECTED
        expected_price = linear.fixed_price

        for resource in known_time_prices:
            if linear.price_for[resource] < 0:
                return SCORE_REJECTED
            expected_price += linear.price_for[resource] * self._expected_time_secs

        # The higher the expected price value, the lower the score.
        # The score is always lower than SCORE_TRUSTED and is always higher than 0.
        score = SCORE_TRUSTED * 1.0 / (expected_price + 1.01)

        return score
