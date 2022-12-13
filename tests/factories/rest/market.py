import datetime
import factory

from ya_market import models as market_models

from tests.factories.props.com import ComLinearPropsFactory
from tests.factories.rest import RestApiModelFactory
from yapapi.rest.market import Agreement, AgreementDetails, OfferProposal, Subscription


class SubscriptionFactory(RestApiModelFactory):
    class Meta:
        model = Subscription

    subscription_id = factory.Faker("pystr")


class RestProposalFactory(factory.Factory):
    class Meta:
        model = market_models.Proposal

    properties = factory.SubFactory(ComLinearPropsFactory)
    constraints = ""
    proposal_id = factory.Faker("pystr")
    issuer_id = factory.Faker("pystr")
    state = "Initial"
    timestamp = factory.LazyFunction(datetime.datetime.now)


class RestProposalEventFactory(factory.Factory):
    class Meta:
        model = market_models.ProposalEvent

    proposal = factory.SubFactory(RestProposalFactory)


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
    proposal = factory.SubFactory(RestProposalEventFactory)


class RestDemandFactory(factory.Factory):
    class Meta:
        model = market_models.Demand

    properties = factory.DictFactory()
    constraints = ""
    demand_id = factory.Faker("pystr")
    requestor_id = factory.Faker("pystr")
    timestamp = factory.LazyFunction(datetime.datetime.now)


class RestOfferFactory(factory.Factory):
    class Meta:
        model = market_models.Offer

    properties = factory.DictFactory()
    constraints = ""
    offer_id = factory.Faker("pystr")
    provider_id = factory.Faker("pystr")
    timestamp = factory.LazyFunction(datetime.datetime.now)


class RestAgreementFactory(factory.Factory):
    class Meta:
        model = market_models.Agreement

    agreement_id = factory.Faker("pystr")
    demand = factory.SubFactory(RestDemandFactory)
    offer = factory.SubFactory(RestOfferFactory)
    valid_to = factory.LazyFunction(lambda: object())
    state = "Approved"
    timestamp = factory.LazyFunction(datetime.datetime.now)


class AgreementDetailsFactory(factory.Factory):
    class Meta:
        model = AgreementDetails

    _ref = factory.SubFactory(RestAgreementFactory)


class AgreementFactory(RestApiModelFactory):
    class Meta:
        model = Agreement

    @factory.post_generation
    def details(obj: Agreement, created, extracted, **kwargs):
        if extracted:
            obj._details = extracted
        else:
            obj._details = AgreementDetailsFactory(**kwargs)

    @factory.post_generation
    def terminated(obj: Agreement, created, extracted, **kwargs):
        assert not kwargs
        if extracted:
            obj._terminated = extracted

    agreement_id = factory.Faker("pystr")
    subscription = factory.SubFactory(SubscriptionFactory)
