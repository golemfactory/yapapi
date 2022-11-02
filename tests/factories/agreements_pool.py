import factory

from yapapi.agreements_pool import BufferedAgreement

from .rest.market import AgreementFactory


class BufferedAgreementFactory(factory.Factory):
    class Meta:
        model = BufferedAgreement

    agreement = factory.SubFactory(AgreementFactory)
    agreement_details = factory.lazy_attribute(lambda o: o.agreement._details)  # noqa
    worker_task = None
    has_multi_activity = False
