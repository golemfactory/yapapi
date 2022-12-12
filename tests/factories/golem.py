import factory
from tests.factories.config import ApiConfigFactory

import yapapi.golem


class GolemFactory(factory.Factory):
    class Meta:
        model = yapapi.golem.Golem
    
    api_config = factory.SubFactory(ApiConfigFactory)
