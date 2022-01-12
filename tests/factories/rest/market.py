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
    def new(cls, provider_id=1, coeffs=(0.001, 0.002, 0.1)):
        kwargs = {}
        kwargs["proposal__proposal__issuer_id"] = provider_id
        kwargs["proposal__proposal__properties__linear_coeffs"] = list(coeffs)
        return cls(**kwargs)

    subscription = factory.SubFactory(SubscriptionFactory)
    proposal = factory.SubFactory(ProposalEventFactory)
