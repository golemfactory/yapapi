import os
from dataclasses import dataclass
from typing import Optional
import ya_market

DEFAULT_API_URL: str = "http://127.0.0.1:7465"


class MissingConfiguration(Exception):
    def __init__(self, key: str, description: str):
        self._key = key
        self._description = description

    def __str__(self):
        return f"Missing configuration for {self._description}. env var is {self._key}"


def env_or_fail(key: str, description: str) -> str:
    val = os.getenv(key)
    if val is None:
        raise MissingConfiguration(key=key, description=description)
    return val


class Configuration(object):
    def __init__(
        self, app_key=None, url: Optional[str] = None, market_url: Optional[str] = None
    ):
        self._app_key: str = app_key or env_or_fail(
            "YAGNA_APPKEY", "API authentication token"
        )
        self._url = url or DEFAULT_API_URL
        self._market_url: str = market_url or os.getenv(
            "YAGNA_MARKET_URL"
        ) or f"{self._url}/market-api/v1"

    @property
    def app_key(self) -> str:
        return self._app_key

    @property
    def market_url(self) -> str:
        return self._market_url

    def market(self) -> ya_market.ApiClient:
        cfg = ya_market.Configuration(host=self.market_url)
        return ya_market.ApiClient(
            configuration=cfg,
            header_name="authorization",
            header_value=f"Bearer {self.app_key}",
        )
