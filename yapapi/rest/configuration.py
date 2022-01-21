import os
from typing import Optional
from typing_extensions import Final
import ya_market  # type: ignore
import ya_payment  # type: ignore
import ya_activity  # type: ignore
import ya_net  # type: ignore

DEFAULT_YAGNA_API_URL: Final[str] = "http://127.0.0.1:7465"


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


# Monkey-patch the `InvoiceReceivedEvent` class to include `eventType`
# field from `InvoiceEvent` (`InvoiceReceivedEvent` inherits from `InvoiceEvent`
# in the API spec, but the generated classes in the Python code are not related)
import ya_payment.models.invoice_received_event


class _InvoiceReceivedEventWithDate(ya_payment.models.invoice_received_event.InvoiceReceivedEvent):
    """A more correct model for InvoiceReceivedEvent message."""

    openapi_types = {
        "event_date": "datetime",
        "invoice_id": "str",
    }

    attribute_map = {
        "event_date": "eventDate",
        "invoice_id": "invoiceId",
    }

    def __init__(self, event_date=None, invoice_id=None, local_vars_configuration=None):
        super().__init__(invoice_id=invoice_id, local_vars_configuration=local_vars_configuration)
        self.event_date = event_date


ya_payment.models.invoice_received_event.InvoiceReceivedEvent = _InvoiceReceivedEventWithDate  # type: ignore
ya_payment.models.InvoiceReceivedEvent = _InvoiceReceivedEventWithDate  # type: ignore


class _DebitNoteReceivedEventWithDate(
    ya_payment.models.debit_note_received_event.DebitNoteReceivedEvent
):
    """A more correct model for DebitNoteReceivedEvent message."""

    openapi_types = {
        "event_date": "datetime",
        "debit_note_id": "str",
    }

    attribute_map = {
        "event_date": "eventDate",
        "debit_note_id": "debitNoteId",
    }

    def __init__(self, event_date=None, debit_note_id=None, local_vars_configuration=None):
        super().__init__(
            debit_note_id=debit_note_id, local_vars_configuration=local_vars_configuration
        )
        self.event_date = event_date


ya_payment.models.debit_note_received_event.DebitNoteReceivedEvent = _DebitNoteReceivedEventWithDate  # type: ignore
ya_payment.models.DebitNoteReceivedEvent = _DebitNoteReceivedEventWithDate  # type: ignore


class Configuration(object):
    """
    REST API's setup and top-level access utility.

    By default, it expects the yagna daemon to be available locally and listening on the
    default port. The urls for the specific APIs are then based on this default base URL.

    It requires one external argument, namely Yagna's application key, which is
    used to authenticate with the daemon. The application key must be either specified
    explicitly using the `app_key` argument or provided by the `YAGNA_APPKEY` environment variable.

    If `YAGNA_API_URL` environment variable exists, it will be used as a base URL
    for all REST API URLs. Example value: http://127.0.10.10:7500 (no trailing slash).

    Other than that, the URLs of each specific REST API can be overridden
    using the following environment variables:
    * `YAGNA_MARKET_URL`
    * `YAGNA_PAYMENT_URL`
    * `YAGNA_ACTIVITY_URL`
    * `YAGNA_NET_URL`
    """

    def __init__(
        self,
        app_key=None,
        *,
        url: Optional[str] = None,
        market_url: Optional[str] = None,
        payment_url: Optional[str] = None,
        activity_url: Optional[str] = None,
        net_url: Optional[str] = None,
    ):
        self.__app_key: str = app_key or env_or_fail("YAGNA_APPKEY", "API authentication token")
        self.__url = url or os.getenv("YAGNA_API_URL") or DEFAULT_YAGNA_API_URL

        def resolve_url(given_url: Optional[str], env_val: str, prefix: str) -> str:
            return given_url or os.getenv(env_val) or f"{self.__url}{prefix}"

        self.__market_url: str = resolve_url(market_url, "YAGNA_MARKET_URL", "/market-api/v1")
        self.__payment_url: str = resolve_url(payment_url, "YAGNA_PAYMENT_URL", "/payment-api/v1")
        self.__activity_url: str = resolve_url(
            activity_url, "YAGNA_ACTIVITY_URL", "/activity-api/v1"
        )
        self.__net_url: str = resolve_url(net_url, "YAGNA_NET_URL", "/net-api/v1")

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
