"""Implementation of strategies for choosing offers from market."""

import abc
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import logging
from types import MappingProxyType
from typing import Dict, Mapping, Optional, Set, Tuple, Union

from dataclasses import dataclass
from typing_extensions import Final

from yapapi.props import com, Activity
from yapapi.props.builder import DemandBuilder, DemandDecorator
from yapapi.props.com import Counter
from yapapi import rest
from yapapi import events


SCORE_NEUTRAL: Final[float] = 0.0
SCORE_REJECTED: Final[float] = -1.0
SCORE_TRUSTED: Final[float] = 100.0


class MarketStrategy(DemandDecorator, abc.ABC):
    """Abstract market strategy."""

    valid_prop_value_ranges: Dict[str, Tuple[Optional[float], Optional[float]]]

    def set_valid_prop_value_ranges(
        self, valid_prop_value_ranges: Dict[str, Tuple[Optional[float], Optional[float]]]
    ) -> None:
        self.valid_prop_value_ranges = valid_prop_value_ranges

    async def answer_to_provider_offer(
        self, our_demand: DemandBuilder, provider_offer: rest.market.OfferProposal
    ) -> DemandBuilder:
        updated_demand = deepcopy(our_demand)
        for prop_name, valid_range in self.valid_prop_value_ranges.items():
            prop_value = provider_offer.props.get(prop_name)
            if prop_value:
                if valid_range[0] is not None and prop_value < valid_range[0]:
                    raise Exception(f"Negotiated property {prop_name} < {valid_range[0]}.")
                if valid_range[1] is not None and prop_value > valid_range[1]:
                    raise Exception(f"Negotiated property {prop_name} > {valid_range[1]}.")
                updated_demand.properties[prop_name] = prop_value
        return updated_demand

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Optionally add relevant constraints to a Demand."""

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        """Score `offer`. Better offers should get higher scores."""
        return SCORE_REJECTED


@dataclass
class DummyMS(MarketStrategy, object):
    """A default market strategy implementation.

    Its :func:`score_offer()` method returns :const:`SCORE_NEUTRAL` for every offer with prices
    that do not exceed maximum prices specified for each counter.
    For other offers, returns :const:`SCORE_REJECTED`.
    """

    def __init__(
        self,
        max_fixed_price: Decimal = Decimal("0.05"),
        max_price_for: Mapping[Union[Counter, str], Decimal] = MappingProxyType({}),
        activity: Optional[Activity] = None,
    ):
        self._max_fixed_price = max_fixed_price
        self._max_price_for: Dict[str, Decimal] = defaultdict(lambda: Decimal("inf"))
        self._max_price_for.update(
            {com.Counter.TIME.value: Decimal("0.002"), com.Counter.CPU.value: Decimal("0.002") * 10}
        )
        self._max_price_for.update(
            {(c.value if isinstance(c, Counter) else c): v for (c, v) in max_price_for.items()}
        )
        self._activity = activity

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        """Ensure that the offer uses `PriceModel.LINEAR` price model."""
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")
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
        """Ensure that the offer uses `PriceModel.LINEAR` price model."""
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


class StrategySupportingMidAgreementPayments(MarketStrategy):
    """Strategy that adds support for negotiating mid-agreement payment properties to the base strategy."""

    base_strategy: MarketStrategy

    def __init__(
        self,
        base_strategy: MarketStrategy,
        valid_prop_value_ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = {},
    ):
        """
        :param base_strategy: the base strategy around which this strategy is wrapped
        """
        self.base_strategy = base_strategy
        self._logger = logging.getLogger(f"{__name__}.{type(self).__name__}")
        if "golem.com.scheme.payu.debit-note-interval-sec?" not in valid_prop_value_ranges:
            valid_prop_value_ranges["golem.com.scheme.payu.debit-note-interval-sec?"] = (20.0, None)
        if "golem.com.scheme.payu.payment-timeout-sec?" not in valid_prop_value_ranges:
            valid_prop_value_ranges["golem.com.scheme.payu.payment-timeout-sec?"] = (None, 3600.0)
        self.set_valid_prop_value_ranges(valid_prop_value_ranges)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        await self.base_strategy.decorate_demand(demand)
        # To enable mid-agreement payments, golem.srv.comp.expiration must be set to a large value.
        demand.add(Activity(expiration=datetime.max))

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        score = await self.base_strategy.score_offer(offer)
        return score
