import factory
from unittest import mock

from yapapi.events import AgreementEvent, AgreementConfirmed, AgreementRejected


class _JobFactory(factory.Factory):
    class Meta:
        model = mock.Mock


class _AgreementFactory(factory.Factory):
    class Meta:
        model = mock.Mock

    @factory.post_generation
    def provider_id(obj: mock.Mock, create, extracted, **kwargs):
        obj.details.raw_details.offer.provider_id = extracted


class AgreementEventFactory(factory.Factory):
    class Meta:
        model = AgreementEvent

    job = factory.SubFactory(_JobFactory)
    agreement = factory.SubFactory(_AgreementFactory)


class AgreementConfirmedFactory(AgreementEventFactory):
    class Meta:
        model = AgreementConfirmed


class AgreementRejectedFactory(AgreementEventFactory):
    class Meta:
        model = AgreementRejected
