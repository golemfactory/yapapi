import os
from typing import Optional
from typing_extensions import Final

import ya_activity  # type: ignore
import ya_market  # type: ignore
import ya_net  # type: ignore
import ya_payment

from yapapi.config import ApiConfig  # type: ignore


class Configuration(object):
    """
    REST API's setup and top-level access utility.

    By default, it expects the yagna daemon to be available locally and listening on the
    default port. The urls for the specific APIs are then based on this default base URL.

    It requires one external argument, namely Yagna's application key, which is
    used to authenticate with the daemon. The application key must be either specified
    explicitly using the `app_key` argument or provided by the `YAGNA_APPKEY` environment variable.
    """

    def __init__(
        self,
        api_config: ApiConfig,
    ):
        self.__app_key: str = api_config.app_key
        self.__url = api_config.api_url

        def resolve_url(given_url: Optional[str], prefix: str) -> str:
            return given_url or f"{self.__url}{prefix}"

        self.__market_url: str = resolve_url(api_config.market_url, "/market-api/v1")
        self.__payment_url: str = resolve_url(api_config.payment_url, "/payment-api/v1")
        self.__activity_url: str = resolve_url(api_config.activity_url, "/activity-api/v1")
        self.__net_url: str = resolve_url(api_config.net_url, "/net-api/v1")

    @property
    def app_key(self) -> str:
        """Yagna daemon's application key used to access the REST API."""
        return self.__app_key

    @property
    def market_url(self) -> str:
        """The URL of the Market REST API"""
        return self.__market_url

    @property
    def payment_url(self) -> str:
        """The URL of the Payment REST API"""
        return self.__payment_url

    @property
    def activity_url(self) -> str:
        """The URL of the Activity REST API"""
        return self.__activity_url

    @property
    def net_url(self) -> str:
        """The URL of the Activity REST API"""
        return self.__net_url

    @property
    def root_url(self) -> str:
        """The root URL of the REST API"""
        return self.__url

    def market(self) -> ya_market.ApiClient:
        """Return a REST client for the Market API."""
        cfg = ya_market.Configuration(host=self.market_url)
        return ya_market.ApiClient(
            configuration=cfg,
            header_name="authorization",
            header_value=f"Bearer {self.app_key}",
        )

    def payment(self) -> ya_payment.ApiClient:
        """Return a REST client for the Payment API."""
        cfg = ya_payment.Configuration(host=self.payment_url)
        return ya_payment.ApiClient(
            configuration=cfg,
            header_name="authorization",
            header_value=f"Bearer {self.app_key}",
        )

    def activity(self) -> ya_activity.ApiClient:
        """Return a REST client for the Activity API."""
        cfg = ya_activity.Configuration(host=self.activity_url)
        return ya_activity.ApiClient(
            configuration=cfg,
            header_name="authorization",
            header_value=f"Bearer {self.app_key}",
        )

    def net(self) -> ya_net.ApiClient:
        """Return a REST client for the Net API."""
        cfg = ya_net.Configuration(host=self.net_url)
        return ya_net.ApiClient(
            configuration=cfg,
            header_name="authorization",
            header_value=f"Bearer {self.app_key}",
        )
