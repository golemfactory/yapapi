from decimal import Decimal
from itertools import product
import pytest
from unittest.mock import Mock

from tests.factories.rest.market import OfferProposalFactory
from yapapi import Golem
from yapapi.props.com import Counter
import yapapi.rest.configuration
from yapapi.strategy import (
    SCORE_NEUTRAL,
    SCORE_REJECTED,
    SCORE_TRUSTED,
    DecreaseScoreForUnconfirmedAgreement,
    LeastExpensiveLinearPayuMS,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_time", [0.1, 1, 60])
async def test_LeastExpensiveLinearPauyuMS_score(expected_time):
    prices = [0.0, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0]

    triples = list(product(prices, repeat=3))  # get triples of (cpu_price, time_price, fixed_price)

    strategy = LeastExpensiveLinearPayuMS(expected_time_secs=expected_time)

    def cost(coeffs):
        return round(coeffs[0] * expected_time + coeffs[1] * expected_time + coeffs[2], 11)

    scores = {
        coeffs: round(
            await strategy.score_offer(OfferProposalFactory(coeffs=coeffs)),
            11,
        )
        for coeffs in triples
    }

    # Sort coeffs by cost, from the most expensive to the least expensive
    triples_by_cost = sorted(triples, key=lambda coeffs: (cost(coeffs), coeffs))
    # Sort coeffs by strategy score, from the lowest (worst) to the highest (best)
    triples_by_score = sorted(triples, key=lambda coeffs: (-scores[coeffs], coeffs))

    assert triples_by_cost == triples_by_score
    assert all(SCORE_NEUTRAL < score < SCORE_TRUSTED for score in scores.values())


@pytest.mark.asyncio
async def test_LeastExpensiveLinearPayuMS_price_caps():
    """Test if LeastExpenciveLinearPayuMS correctly handles price caps."""

    prices = [0.0, 0.01, 1.0, 100.0]
    epsilon = 0.0000001

    triples = list(product(prices, repeat=3))  # get triples of (cpu_price, time_price, fixed_price)

    for cpu_price, time_price, fixed_price in triples:

        offer = OfferProposalFactory(coeffs=(cpu_price, time_price, fixed_price))

        async def _test_strategy(strategy, cpu_price_cap, time_price_cap, fixed_price_cap):
            score = await strategy.score_offer(offer)
            should_reject = (
                (cpu_price_cap is not None and cpu_price > cpu_price_cap)
                or (time_price_cap is not None and time_price > time_price_cap)
                or (fixed_price_cap is not None and fixed_price > fixed_price_cap)
            )
            assert should_reject == (score == SCORE_REJECTED), (
                f"failed for cpu_price_cap = {cpu_price_cap}, cpu_price = {cpu_price}, "
                f"time_price_cap = {time_price_cap}, time_price = {time_price}, "
                f"fixed_price_cap = {fixed_price_cap}, fixed_price = {fixed_price}, "
                f"score = {score}"
            )

        for cpu_price_cap, time_price_cap, fixed_price_cap in product(
            (None, cpu_price - epsilon, cpu_price, cpu_price + epsilon),
            (None, time_price - epsilon, time_price, time_price + epsilon),
            (None, fixed_price - epsilon, fixed_price, fixed_price + epsilon),
        ):
            if cpu_price_cap == time_price_cap == fixed_price_cap == None:
                strategies = (
                    LeastExpensiveLinearPayuMS(),
                    LeastExpensiveLinearPayuMS(max_price_for={}),
                )
            elif cpu_price_cap == time_price_cap == None:
                strategies = (
                    LeastExpensiveLinearPayuMS(max_fixed_price=fixed_price_cap),
                    LeastExpensiveLinearPayuMS(max_fixed_price=fixed_price_cap, max_price_for={}),
                )
            else:
                counter_caps = {}
                if cpu_price_cap is not None:
                    counter_caps[Counter.CPU] = cpu_price_cap
                if time_price_cap is not None:
                    counter_caps[Counter.TIME] = time_price_cap

                if fixed_price_cap is None:
                    strategies = (
                        LeastExpensiveLinearPayuMS(max_price_for=counter_caps),
                        # Also add a mock cap for an unrelated counter
                        LeastExpensiveLinearPayuMS(
                            max_price_for={**counter_caps, Counter.STORAGE: Decimal(0.0)}
                        ),
                    )
                else:
                    strategies = (
                        LeastExpensiveLinearPayuMS(
                            max_fixed_price=fixed_price_cap, max_price_for=counter_caps
                        ),
                    )

            for strategy in strategies:
                await _test_strategy(strategy, cpu_price_cap, time_price_cap, fixed_price_cap)


@pytest.mark.asyncio
async def test_default_strategy_type(monkeypatch, golem_factory):
    """Test if the default strategy is composed of appropriate `MarketStrategy` subclasses."""

    monkeypatch.setattr(yapapi.rest, "Configuration", Mock)

    golem = golem_factory(budget=1.0)
    default_strategy = golem.strategy
    assert isinstance(default_strategy, DecreaseScoreForUnconfirmedAgreement)
    assert isinstance(default_strategy.base_strategy, LeastExpensiveLinearPayuMS)


@pytest.mark.asyncio
async def test_user_strategy_not_modified(monkeypatch, golem_factory):
    """Test that a user strategy is not wrapped in `DecreaseScoreForUnconfirmedAgreement`."""

    monkeypatch.setattr(yapapi.rest, "Configuration", Mock)

    user_strategy = Mock()
    golem = golem_factory(budget=1.0, strategy=user_strategy)
    assert golem.strategy == user_strategy


class TestLeastExpensiveLinearPayuMS:
    @pytest.fixture(autouse=True)
    def _strategy(self):
        self.strategy = LeastExpensiveLinearPayuMS(expected_time_secs=60)

    @pytest.mark.asyncio
    async def test_same_score(self):
        coeffs = [0.001, 0.002, 0.1]
        offer1 = OfferProposalFactory(coeffs=coeffs)
        offer2 = OfferProposalFactory(coeffs=coeffs)

        assert await self.strategy.score_offer(offer1) == await self.strategy.score_offer(offer2)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "coeffs", [[-0.001, 0.002, 0.1], [0.001, -0.002, 0.1], [0.001, 0.002, -0.1]]
    )
    async def test_score_negative_coeff(self, coeffs):
        offer = OfferProposalFactory(coeffs=coeffs)
        assert await self.strategy.score_offer(offer) == SCORE_REJECTED
