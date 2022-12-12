import factory

import yapapi.config


class ApiConfigFactory(factory.Factory):
    class Meta:
        model = yapapi.config.ApiConfig

    app_key = "yagna-app-key"
