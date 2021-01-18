from itertools import product
import pytest

from yapapi.executor.strategy import (
    LeastExpensiveLinearPayuMS,
    SCORE_TRUSTED,
    SCORE_REJECTED,
    SCORE_NEUTRAL,
)
from yapapi.props import com

from tests.factories.rest.market import OfferProposalFactory


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
            await strategy.score_offer(
                OfferProposalFactory(**{"proposal__proposal__properties__linear_coeffs": coeffs})
            ),
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


class TestLeastExpensiveLinearPayuMS:
    @pytest.fixture(autouse=True)
    def _strategy(self):
        self.strategy = LeastExpensiveLinearPayuMS(expected_time_secs=60)

    @pytest.mark.asyncio
    async def test_same_score(self):
        coeffs = [0.001, 0.002, 0.1]
        offer1 = OfferProposalFactory(**{"proposal__proposal__properties__linear_coeffs": coeffs})
        offer2 = OfferProposalFactory(**{"proposal__proposal__properties__linear_coeffs": coeffs})

        assert await self.strategy.score_offer(offer1) == await self.strategy.score_offer(offer2)

    @pytest.mark.asyncio
    async def test_score_unknown_price(self):
        offer = OfferProposalFactory(
            **{
                "proposal__proposal__properties__defined_usages": [
                    com.Counter.MAXMEM.value,
                    com.Counter.TIME.value,
                ]
            }
        )
        assert await self.strategy.score_offer(offer) == SCORE_REJECTED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "coeffs", [[-0.001, 0.002, 0.1], [0.001, -0.002, 0.1], [0.001, 0.002, -0.1]]
    )
    async def test_score_negative_coeff(self, coeffs):
        offer = OfferProposalFactory(**{"proposal__proposal__properties__linear_coeffs": coeffs})
        assert await self.strategy.score_offer(offer) == SCORE_REJECTED
