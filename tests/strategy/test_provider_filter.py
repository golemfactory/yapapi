import pytest

from tests.factories.rest.market import OfferProposalFactory
from yapapi import Golem
from yapapi.contrib.strategy import ProviderFilter
from yapapi.strategy import SCORE_REJECTED

from .helpers import DEFAULT_OFFER_SCORE, Always6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_providers",
    ((), (1,), (2,), (1, 2)),
)
async def test_restricted_providers(bad_providers):
    """Test if the strategy restricts correct providers."""

    strategy = ProviderFilter(Always6(), lambda provider_id: provider_id not in bad_providers)

    for provider_id in (1, 2, 3):
        offer = OfferProposalFactory(provider_id=provider_id)
        expected_score = SCORE_REJECTED if provider_id in bad_providers else 6
        assert expected_score == await strategy.score_offer(offer)


@pytest.mark.asyncio
async def test_dynamic_change():
    """Test if changes in the is_allowed function are reflected in scores."""

    something_happened = False

    def is_allowed(provider_id):
        if something_happened:
            return False
        return True

    strategy = ProviderFilter(Always6(), is_allowed)

    for i in range(0, 5):
        offer = OfferProposalFactory(provider_id=1)
        expected_score = SCORE_REJECTED if i > 3 else 6
        assert expected_score == await strategy.score_offer(offer)

        if i == 3:
            something_happened = True


@pytest.mark.asyncio
async def test_default_strategy_update(dummy_yagna_engine):
    """Test if default strategy extended with ProviderFilter works as expected."""

    golem = Golem(budget=1, app_key="NOT_A_REAL_APPKEY")
    golem.strategy = ProviderFilter(golem.strategy, lambda provider_id: provider_id == 2)

    async with golem:
        for provider_id in (1, 2, 3):
            offer = OfferProposalFactory(provider_id=provider_id)
            expected_score = DEFAULT_OFFER_SCORE if provider_id == 2 else SCORE_REJECTED
            assert expected_score == await golem._engine._strategy.score_offer(offer)
