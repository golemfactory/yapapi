import factory
from unittest import mock

from ya_market.api.requestor_api import RequestorApi


class RestApiModelFactory(factory.Factory):
    api = factory.LazyFunction(lambda: RequestorApi(mock.Mock()))
