import yapapi.config
import yapapi.golem


def golem_factory(**kwargs) -> yapapi.golem.Golem:
    if "api_config" not in kwargs:
        kwargs["api_config"] = yapapi.config.ApiConfig(app_key="yagna-app-key")
    return yapapi.golem.Golem(**kwargs)
