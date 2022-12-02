from dataclasses import dataclass, field
from functools import partial
import os
from typing import Optional


class MissingConfiguration(Exception):
    def __init__(self, key: str, description: str):
        self._key = key
        self._description = description

    def __str__(self):
        return f"Missing configuration for {self._description}. Please set env var {self._key}."


@dataclass
class ApiConfig:
    """
    Yagna low level API configuration
    Attributes:
        app_key: Yagna application key.
            If not provided, the default is to get the value from `YAGNA_APPKEY` environment variable.
            If no value will be found MissingConfiguration error will be thrown
        api_url: base URL or all REST API URLs. Example value: http://127.0.10.10:7500 (no trailing slash).
            Uses YAGNA_API_URL environment variable
        market_url: If not provided `api_url` will be used to construct it.
            Uses YAGNA_MARKET_URL environment variable
        payment_url: If not provided `api_url` will be used to construct it.
            Uses YAGNA_PAYMENT_URL environment variable
        net_url: Uses If not provided `api_url` will be used to construct it.
            YAGNA_NET_URL environment variable
        activity_url: If not provided `api_url` will be used to construct it.
            Uses YAGNA_ACTIVITY_URL environment variable
    """

    app_key: str = field(default_factory=partial(os.getenv, "YAGNA_APPKEY"))  # type: ignore
    api_url: Optional[str] = field(default_factory=partial(os.getenv, "YAGNA_API_URL"))
    market_url: Optional[str] = field(default_factory=partial(os.getenv, "YAGNA_MARKET_URL"))
    payment_url: Optional[str] = field(default_factory=partial(os.getenv, "YAGNA_PAYMENT_URL"))
    net_url: Optional[str] = field(default_factory=partial(os.getenv, "YAGNA_NET_URL"))
    activity_url: Optional[str] = field(default_factory=partial(os.getenv, "YAGNA_ACTIVITY_URL"))

    def __post_init__(self):
        if self.app_key is None:
            raise MissingConfiguration(key="YAGNA_APPKEY", description="API authentication token")
