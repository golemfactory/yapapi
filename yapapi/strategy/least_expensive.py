from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
import logging
from types import MappingProxyType
from typing import Dict, Mapping, Union

from yapapi import rest
from yapapi.props import com
from yapapi.props.builder import DemandBuilder
from yapapi.props.com import Counter

from .base import SCORE_REJECTED, SCORE_TRUSTED, MarketStrategy


@dataclass
class LeastExpensiveLinearPayuMS(MarketStrategy, object):
    """A strategy that scores offers according to cost for given computation time."""

    def __init__(
        self,
        expected_time_secs: int = 60,
        max_fixed_price: Decimal = Decimal("inf"),
        max_price_for: Mapping[Union[Counter, str], Decimal] = MappingProxyType({}),
    ):
        self._expected_time_secs = expected_time_secs
        self._logger = logging.getLogger(f"{__name__}.{type(self).__name__}")
        self._max_fixed_price = max_fixed_price
        self._max_price_for: Dict[str, Decimal] = defaultdict(lambda: Decimal("inf"))
        self._max_price_for.update(
            {(c.value if isinstance(c, Counter) else c): v for (c, v) in max_price_for.items()}
        )

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        await super().decorate_demand(demand)

        # Ensure that the offer uses `PriceModel.LINEAR` price model.
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer` according to cost for expected computation time."""

        linear: com.ComLinear = com.ComLinear.from_properties(offer.props)
        self._logger.debug("Scoring offer %s, parameters: %s", offer.id, linear)
        if linear.scheme != com.BillingScheme.PAYU:
            self._logger.debug(
                "Rejected offer %s: unsupported scheme '%s'", offer.id, linear.scheme
            )
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

        expected_usage = []

        for resource in linear.usage_vector:

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

            expected_usage.append(self._expected_time_secs)

        # The higher the expected price value, the lower the score.
        # The score is always lower than SCORE_TRUSTED and is always higher than 0.
        score = SCORE_TRUSTED * 1.0 / (linear.calculate_cost(expected_usage) + 1.01)
        return score
