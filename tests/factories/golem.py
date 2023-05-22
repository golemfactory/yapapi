import factory

import yapapi.golem
from tests.factories.config import ApiConfigFactory


class GolemFactory(factory.Factory):
    class Meta:
        model = yapapi.golem.Golem

    budget = 10.0
    api_config = factory.SubFactory(ApiConfigFactory)
