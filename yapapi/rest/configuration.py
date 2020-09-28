import os
from typing import Optional
from typing_extensions import Final
import ya_market  # type: ignore
import ya_payment  # type: ignore
import ya_activity  # type: ignore

DEFAULT_API_URL: Final[str] = "http://127.0.0.1:7465"


class MissingConfiguration(Exception):
    def __init__(self, key: str, description: str):
        self._key = key
        self._description = description

    def __str__(self):
        return f"Missing configuration for {self._description}. Please set env var {self._key}."


def env_or_fail(key: str, description: str) -> str:
    val = os.getenv(key)
    if val is None:
        raise MissingConfiguration(key=key, description=description)
    return val


class Configuration(object):
    def __init__(
        self,
        app_key=None,
        *,
        url: Optional[str] = None,
        market_url: Optional[str] = None,
        payment_url: Optional[str] = None,
        activity_url: Optional[str] = None,
    ):
        self.__app_key: str = app_key or env_or_fail("YAGNA_APPKEY", "API authentication token")
        self.__url = url or DEFAULT_API_URL

        def resolve_url(given_url: Optional[str], env_val: str, prefix: str) -> str:
            return given_url or os.getenv(env_val) or f"{self.__url}{prefix}"

        self.__market_url: str = resolve_url(market_url, "YAGNA_MARKET_URL", "/market-api/v1")
        self.__payment_url: str = resolve_url(payment_url, "YAGNA_PAYMENT_URL", "/payment-api/v1")
        self.__activity_url: str = resolve_url(
            activity_url, "YAGNA_ACTIVITY_URL", "/activity-api/v1"
        )

    @property
    def app_key(self) -> str:
        return self.__app_key

    @property
    def market_url(self) -> str:
        return self.__market_url

    @property
    def payment_url(self) -> str:
        return self.__payment_url

    @property
    def activity_url(self) -> str:
        return self.__activity_url

    def market(self) -> ya_market.ApiClient:
        cfg = ya_market.Configuration(host=self.market_url)
        return ya_market.ApiClient(
            configuration=cfg, header_name="authorization", header_value=f"Bearer {self.app_key}",
        )

    def payment(self) -> ya_payment.ApiClient:
        cfg = ya_payment.Configuration(host=self.payment_url)
        return ya_payment.ApiClient(
            configuration=cfg, header_name="authorization", header_value=f"Bearer {self.app_key}",
        )

    def activity(self) -> ya_activity.ApiClient:
        cfg = ya_activity.Configuration(host=self.activity_url)
        return ya_activity.ApiClient(
            configuration=cfg, header_name="authorization", header_value=f"Bearer {self.app_key}",
        )
