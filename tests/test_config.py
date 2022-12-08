import os
import pytest

import yapapi.config


def test_api_config_missing_configuration_error(purge_yagna_os_env):
    with pytest.raises(yapapi.config.MissingConfiguration):
        yapapi.config.ApiConfig()


def test_api_config_explicit(purge_yagna_os_env):
    given_config_kwargs = {
        "app_key": "yagna-app-key",
        "api_url": "yagna-api_url",
        "market_url": "yagna-market_url",
        "payment_url": "yagna-payment_url",
        "net_url": "yagna-net_url",
        "activity_url": "yagna-activity_url",
    }
    received_config = yapapi.config.ApiConfig(**given_config_kwargs)
    for key, value in given_config_kwargs.items():
        assert getattr(received_config, key) == value


def test_api_config_only_app_key(purge_yagna_os_env):
    os.environ["YAGNA_APPKEY"] = "yagna-app-key"
    config = yapapi.config.ApiConfig()
    assert config.app_key == "yagna-app-key"
    assert config.api_url is not None
    assert config.market_url is None
    assert config.payment_url is None
    assert config.net_url is None
    assert config.activity_url is None


def test_api_config_from_env(purge_yagna_os_env):
    given_config_vars = {
        "app_key": "YAGNA_APPKEY",
        "api_url": "YAGNA_API_URL",
        "market_url": "YAGNA_MARKET_URL",
        "payment_url": "YAGNA_PAYMENT_URL",
        "net_url": "YAGNA_NET_URL",
        "activity_url": "YAGNA_ACTIVITY_URL",
    }
    for _, env_var in given_config_vars.items():
        os.environ[env_var] = env_var
    config = yapapi.config.ApiConfig()
    for key, value in given_config_vars.items():
        assert getattr(config, key) == value
