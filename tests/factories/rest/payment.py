import datetime
from unittest import mock

import factory

from ya_payment import models as payment_models
from ya_payment.api.requestor_api import RequestorApi

from yapapi.rest.payment import DebitNote


class RestDebitNoteFactory(factory.Factory):
    class Meta:
        model = payment_models.DebitNote

    debit_note_id = factory.Faker("pystr")
    issuer_id = factory.Faker("pystr")
    recipient_id = factory.Faker("pystr")
    payee_addr = factory.Faker("pystr")
    payer_addr = factory.Faker("pystr")
    payment_platform = factory.Faker("pystr")
    timestamp = factory.LazyFunction(datetime.datetime.now)
    agreement_id = factory.Faker("pystr")
    activity_id = factory.Faker("pystr")
    total_amount_due = factory.Sequence(lambda n: str(n))
    status = "RECEIVED"


class DebitNoteFactory(factory.Factory):
    class Meta:
        model = DebitNote

    _api = factory.LazyFunction(lambda: RequestorApi(mock.Mock()))
    _base = factory.SubFactory(RestDebitNoteFactory)
