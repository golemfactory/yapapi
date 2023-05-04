from collections import defaultdict
from decimal import Decimal
from types import MappingProxyType
from typing import Dict, Mapping, Optional, Union

from dataclasses import dataclass
from deprecated import deprecated

from golem_core.core.market_api import Activity, DemandBuilder

from yapapi import rest
from yapapi.props import com

from .base import SCORE_NEUTRAL, SCORE_REJECTED, MarketStrategy


@deprecated(version="0.9.0", reason="Use `LeastExpensiveLinearPayuMS` instead.")
@dataclass
class DummyMS(MarketStrategy, object):
    """A default market strategy implementation.

    [ DEPRECATED, use `LeastExpensiveLinearPayuMS` instead ]

    Its :func:`score_offer()` method returns :const:`SCORE_NEUTRAL` for every offer with prices
    that do not exceed maximum prices specified for each counter.
    For other offers, returns :const:`SCORE_REJECTED`.
    """

    def __init__(
        self,
        max_fixed_price: Decimal = Decimal("0.05"),
        max_price_for: Mapping[Union[com.Counter, str], Decimal] = MappingProxyType({}),
        activity: Optional[Activity] = None,
    ):
        self._max_fixed_price = max_fixed_price
        self._max_price_for: Dict[str, Decimal] = defaultdict(lambda: Decimal("inf"))
        self._max_price_for.update(
            {com.Counter.TIME.value: Decimal("0.002"), com.Counter.CPU.value: Decimal("0.002") * 10}
        )
        self._max_price_for.update(
            {(c.value if isinstance(c, com.Counter) else c): v for (c, v) in max_price_for.items()}
        )
        self._activity = activity

    async def decorate_demand_builder(self, demand: DemandBuilder) -> None:
        await super().decorate_demand_builder(demand)

        # Ensure that the offer uses `PriceModel.LINEAR` price model
        demand.add_constraints(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")
        self._activity = Activity.from_properties(demand.properties)

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer`. Returns either `SCORE_REJECTED` or `SCORE_NEUTRAL`."""

        linear: com.ComLinear = com.ComLinear.from_properties(offer.props)

        if linear.scheme != com.BillingScheme.PAYU:
            return SCORE_REJECTED

        if linear.fixed_price > self._max_fixed_price:
            return SCORE_REJECTED
        for counter, price in linear.price_for.items():
            if counter not in self._max_price_for:
                return SCORE_REJECTED
            if price > self._max_price_for[counter]:
                return SCORE_REJECTED

        return SCORE_NEUTRAL
