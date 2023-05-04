from unittest import mock

import factory

from ya_market.api.requestor_api import RequestorApi


class RestApiModelFactory(factory.Factory):
    api = factory.LazyFunction(lambda: RequestorApi(mock.Mock()))
