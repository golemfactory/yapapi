import factory

import yapapi.golem


class GolemFactory(factory.Factory):
    class Meta:
        model = yapapi.golem.Golem
