import pytest

from yapapi.executor.strategy import LeastExpensiveLinearPayuMS

from tests.factories.rest.market import OfferProposalFactory


class TestStrategy:
    @pytest.fixture(autouse=True)
    def _strategy(self):
        self.strategy = LeastExpensiveLinearPayuMS(expected_time_secs=60)

    @pytest.mark.asyncio
    async def test_strategy_same(self):
        coeffs = [0.001, 0.002, 0.0]
        offer1 = OfferProposalFactory(**{"proposal__proposal__properties__linear_coeffs": coeffs})
        offer2 = OfferProposalFactory(**{"proposal__proposal__properties__linear_coeffs": coeffs})

        assert await self.strategy.score_offer(offer1) == await self.strategy.score_offer(offer2)

    @pytest.mark.asyncio
    async def test_strategy_different_variable_price(self):
        cheaper = OfferProposalFactory(
            **{"proposal__proposal__properties__linear_coeffs": [0.001, 0.002, 0.0]}
        )
        more_expensive = OfferProposalFactory(
            **{"proposal__proposal__properties__linear_coeffs": [0.01, 0.02, 0.0]}
        )

        assert await self.strategy.score_offer(cheaper) > await self.strategy.score_offer(
            more_expensive
        )

    @pytest.mark.asyncio
    async def test_strategy_different_fixed_price(self):
        cheaper = OfferProposalFactory(
            **{"proposal__proposal__properties__linear_coeffs": [0.001, 0.002, 0.0]}
        )
        more_expensive = OfferProposalFactory(
            **{"proposal__proposal__properties__linear_coeffs": [0.001, 0.002, 0.1]}
        )

        assert await self.strategy.score_offer(cheaper) > await self.strategy.score_offer(
            more_expensive
        )

    @pytest.mark.asyncio
    async def test_strategy_different_fixed_and_variable(self):
        more_expensive = OfferProposalFactory(
            **{"proposal__proposal__properties__linear_coeffs": [0.1, 0.1, 0.0]}
        )
        cheaper = OfferProposalFactory(
            **{"proposal__proposal__properties__linear_coeffs": [0, 0, 1.0]}
        )
        # we have set the time at 60s

        assert await self.strategy.score_offer(cheaper) > await self.strategy.score_offer(
            more_expensive
        )


@pytest.mark.asyncio
async def test_strategy_different_short_time():
    strategy = LeastExpensiveLinearPayuMS(expected_time_secs=1)
    cheaper = OfferProposalFactory(
        **{"proposal__proposal__properties__linear_coeffs": [0.1, 0.1, 0.0]}
    )
    more_expensive = OfferProposalFactory(
        **{"proposal__proposal__properties__linear_coeffs": [0, 0, 1.0]}
    )
    # with time at 1s, the fixed cost prevails

    assert await strategy.score_offer(cheaper) > await strategy.score_offer(more_expensive)
