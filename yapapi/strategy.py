"""Implementation of strategies for choosing offers from market."""

import abc
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
from types import MappingProxyType
from typing import Dict, Mapping, Optional, Set, Tuple, Union, NamedTuple

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

PROP_DEBIT_NOTE_INTERVAL_SEC: Final[str] = "golem.com.scheme.payu.debit-note.interval-sec?"
PROP_PAYMENT_TIMEOUT_SEC: Final[str] = "golem.com.scheme.payu.payment-timeout-sec?"
PROP_DEBIT_NOTE_ACCEPTANCE_TIMEOUT: Final[str] = "golem.com.payment.debit-notes.accept-timeout?"

MID_AGREEMENT_PAYMENTS_PROPS = [PROP_DEBIT_NOTE_INTERVAL_SEC, PROP_PAYMENT_TIMEOUT_SEC]


@dataclass
class PropValueRange:
    min: Optional[float] = None
    max: Optional[float] = None

    def __contains__(self, item: float) -> bool:
        return (self.min is None or item >= self.min) and (self.max is None or item <= self.max)

    def closest_acceptable(self, item: float):
        if self.min is not None and item < self.min:
            return self.min
        if self.max is not None and item > self.max:
            return self.max
        return item

    def __str__(self):
        return f"({self.min}, {self.max})"


DEFAULT_DEBIT_NOTE_ACCEPTANCE_TIMEOUT_SEC: Final[int] = 30
DEFAULT_DEBIT_NOTE_INTERVAL_SEC: Final[int] = 60
DEFAULT_PAYMENT_TIMEOUT_SEC: Final[int] = 18000
MIN_EXPIRATION_FOR_MID_AGREEMENT_PAYMENTS: Final[int] = DEFAULT_PAYMENT_TIMEOUT_SEC

DEBIT_NOTE_INTERVAL_GRACE_PERIOD: Final[int] = 30


DEFAULT_PROPERTY_VALUE_RANGES: Dict[str, PropValueRange] = {
    PROP_DEBIT_NOTE_INTERVAL_SEC: PropValueRange(DEFAULT_DEBIT_NOTE_INTERVAL_SEC, None),
    PROP_PAYMENT_TIMEOUT_SEC: PropValueRange(DEFAULT_PAYMENT_TIMEOUT_SEC, None),
    PROP_DEBIT_NOTE_ACCEPTANCE_TIMEOUT: PropValueRange(
        DEFAULT_DEBIT_NOTE_ACCEPTANCE_TIMEOUT_SEC, None
    ),
}

logger = logging.getLogger(__name__)


class MarketStrategy(DemandDecorator, abc.ABC):
    """Abstract market strategy."""

    acceptable_prop_value_ranges: Dict[str, PropValueRange]

    def set_prop_value_ranges_defaults(
        self, acceptable_prop_value_ranges: Dict[str, PropValueRange]
    ) -> None:
        try:
            value_ranges = self.acceptable_prop_value_ranges
            for key, value in acceptable_prop_value_ranges.items():
                if key not in value_ranges:
                    value_ranges[key] = value
        except AttributeError:
            self.acceptable_prop_value_ranges = acceptable_prop_value_ranges.copy()

    async def respond_to_provider_offer(
        self,
        our_demand: DemandBuilder,
        provider_offer: rest.market.OfferProposal,
    ) -> DemandBuilder:
        # Create a new DemandBuilder with a response to a provider offer.
        updated_demand = deepcopy(our_demand)

        # Set default property value ranges if they were not set in the market strategy.
        self.set_prop_value_ranges_defaults(DEFAULT_PROPERTY_VALUE_RANGES)

        # only enable mid-agreement-payments when we need a longer expiration
        # and when the provider supports it
        activity = Activity.from_properties(our_demand.properties)

        assert activity.expiration
        expiration_secs = round((activity.expiration - datetime.now(timezone.utc)).total_seconds())
        trigger_mid_agreement_payments = expiration_secs >= float(
            MIN_EXPIRATION_FOR_MID_AGREEMENT_PAYMENTS
        )

        mid_agreement_payments_enabled = (
            PROP_DEBIT_NOTE_INTERVAL_SEC in provider_offer.props and trigger_mid_agreement_payments
        )

        if mid_agreement_payments_enabled:
            logger.debug(
                "Enabling mid-agreement payments mechanism "
                "as the expiration set to %ss (more than %ss).",
                expiration_secs,
                MIN_EXPIRATION_FOR_MID_AGREEMENT_PAYMENTS,
            )
        elif trigger_mid_agreement_payments:
            logger.debug(
                "Expiration of %ss (more than our minimum for MAP: %ss) while negotiating "
                "with a provider unaware of mid-agreeement-payments.",
                expiration_secs,
                MIN_EXPIRATION_FOR_MID_AGREEMENT_PAYMENTS,
            )

        # Update response by either accepting proposed values or by proposing ours
        # only set mid-agreement payments values if we're agreeing to them
        for prop_key, acceptable_range in self.acceptable_prop_value_ranges.items():
            prop_value = provider_offer.props.get(prop_key)
            if prop_value and (
                mid_agreement_payments_enabled or prop_key not in MID_AGREEMENT_PAYMENTS_PROPS
            ):

                if prop_value not in acceptable_range:
                    our_value = acceptable_range.closest_acceptable(prop_value)
                    logger.debug(
                        f"Negotiated property %s = %s outside of our accepted range: %s. "
                        f"Proposing our own value instead: %s",
                        prop_key,
                        prop_value,
                        acceptable_range,
                        our_value,
                    )
                    prop_value = our_value

                updated_demand.properties[prop_key] = prop_value
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
        await super().decorate_demand(demand)

        # Ensure that the offer uses `PriceModel.LINEAR` price model
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
