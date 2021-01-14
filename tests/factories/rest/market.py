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

    subscription = factory.SubFactory(SubscriptionFactory)
    proposal = factory.SubFactory(ProposalEventFactory)
