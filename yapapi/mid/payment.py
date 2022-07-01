from abc import ABC

from ya_payment import RequestorApi, models as ya_models

from .golem_object import GolemObject


class PaymentApiObject(GolemObject, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_payment_api)

    @property
    def model(self):
        my_name = type(self).__name__
        return ya_models.getattr(my_name)

    @classmethod
    def requestor_api(cls, node):
        return RequestorApi(node._ya_payment_api)


class Allocation(PaymentApiObject):
    pass
