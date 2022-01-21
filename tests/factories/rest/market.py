import datetime
import factory
from unittest import mock

from ya_market.api.requestor_api import RequestorApi
from ya_market.models import Proposal, ProposalEvent
from yapapi.rest.market import OfferProposal, Subscription

from tests.factories.props.com import ComLinearPropsFactory


class SubscriptionFactory(factory.Factory):
    class Meta:
        model = Subscription

    api = factory.LazyFunction(lambda: RequestorApi(mock.Mock()))
    subscription_id = factory.Faker("pystr")


class ProposalFactory(factory.Factory):
    class Meta:
        model = Proposal

    properties = factory.SubFactory(ComLinearPropsFactory)
    constraints = ""
    proposal_id = factory.Faker("pystr")
    issuer_id = factory.Faker("pystr")
    state = "Initial"
    timestamp = factory.LazyFunction(datetime.datetime.now)


class ProposalEventFactory(factory.Factory):
    class Meta:
        model = ProposalEvent

    proposal = factory.SubFactory(ProposalFactory)


class OfferProposalFactory(factory.Factory):
    class Meta:
        model = OfferProposal

    @classmethod
    def create(cls, provider_id=None, coeffs=None, **kwargs):
        if provider_id:
            kwargs["proposal__proposal__issuer_id"] = provider_id
        if coeffs:
            kwargs["proposal__proposal__properties__linear_coeffs"] = list(coeffs)
        return super().create(**kwargs)

    subscription = factory.SubFactory(SubscriptionFactory)
    proposal = factory.SubFactory(ProposalEventFactory)
