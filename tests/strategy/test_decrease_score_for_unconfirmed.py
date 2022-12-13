import asyncio
import pytest

from tests.factories.events import AgreementConfirmedFactory as AgreementConfirmed
from tests.factories.events import AgreementRejectedFactory as AgreementRejected
from tests.factories.rest.market import OfferProposalFactory
from yapapi import Golem
from yapapi.strategy import DecreaseScoreForUnconfirmedAgreement

from .helpers import DEFAULT_OFFER_SCORE, Always6

#   (events, providers_with_decreased_scores)
sample_data = (
    ((), ()),
    (((AgreementRejected, 1),), (1,)),
    (((AgreementRejected, 2),), (2,)),
    (((AgreementConfirmed, 1),), ()),
    (((AgreementRejected, 1), (AgreementConfirmed, 1)), ()),
    (((AgreementRejected, 2), (AgreementConfirmed, 1)), (2,)),
    (((AgreementRejected, 1), (AgreementConfirmed, 2)), (1,)),
    (((AgreementRejected, 1), (AgreementRejected, 1)), (1,)),
    (((AgreementRejected, 1), (AgreementConfirmed, 1), (AgreementRejected, 1)), (1,)),
)


@pytest.mark.asyncio
async def test_6():
    strategy = DecreaseScoreForUnconfirmedAgreement(Always6(), 0.5)
    offer = OfferProposalFactory()
    assert 6 == await strategy.score_offer(offer)


@pytest.mark.asyncio
@pytest.mark.parametrize("events_def, decreased_providers", sample_data)
async def test_decrease_score_for(events_def, decreased_providers):
    """Test if DecreaseScoreForUnconfirmedAgreement works as expected"""
    strategy = DecreaseScoreForUnconfirmedAgreement(Always6(), 0.5)

    for event_cls, event_provider_id in events_def:
        event = event_cls(agreement__provider_id=event_provider_id)
        strategy.on_event(event)

    for provider_id in (1, 2):
        offer = OfferProposalFactory(provider_id=provider_id)
        expected_score = 3 if provider_id in decreased_providers else 6
        assert expected_score == await strategy.score_offer(offer)


def empty_event_consumer(event):
    """To silience the default logger - it doesn't work with mocked events"""
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("events_def, decreased_providers", sample_data)
async def test_full_DSFUA_workflow(dummy_yagna_engine, events_def, decreased_providers):
    """Test if DecreaseScoreForUnconfirmedAgreement is correctly initialized as a default strategy

    that is - if events emitted by the engine reach the event consumer of the default strategy"""

    golem = Golem(budget=1, event_consumer=empty_event_consumer, app_key="NOT_A_REAL_APPKEY")
    async with golem:
        for event_cls, event_provider_id in events_def:
            event = event_cls(agreement__provider_id=event_provider_id)
            golem._engine._emit_event(event)

        await asyncio.sleep(0.1)  # let the events propagate

        for provider_id in (1, 2):
            offer = OfferProposalFactory(provider_id=provider_id)
            expected_score = (
                DEFAULT_OFFER_SCORE / 2
                if provider_id in decreased_providers
                else DEFAULT_OFFER_SCORE
            )
            assert expected_score == await golem._engine._strategy.score_offer(offer)
