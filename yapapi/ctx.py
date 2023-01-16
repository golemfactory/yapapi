import enum
import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type

from dataclasses import dataclass, field

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

from ya_activity.models import ActivityState as yaa_ActivityState
from ya_activity.models import ActivityUsage as yaa_ActivityUsage

from yapapi.events import ActivityEventType, CommandExecuted
from yapapi.props.com import ComLinear
from yapapi.rest.activity import Activity
from yapapi.rest.market import Agreement, AgreementDetails
from yapapi.script import Script
from yapapi.storage import DOWNLOAD_BYTES_LIMIT_DEFAULT, StorageProvider
from yapapi.utils import get_local_timezone

logger = logging.getLogger(__name__)


@dataclass
class ExecOptions:
    """Options related to command batch execution."""

    wait_for_results: bool = True
    batch_timeout: Optional[timedelta] = None


class WorkContextEmitter(Protocol):
    def __call__(self, event_class: Type[ActivityEventType], **kwargs) -> ActivityEventType:
        ...


class WorkContext:
    """Provider node's work context.

    Used to schedule commands to be sent to the provider and enable other interactions between the
    requestor agent's client code and the activity on provider's end.
    """

    def __init__(
        self,
        activity: Activity,
        agreement: Agreement,
        storage: StorageProvider,
        emitter: WorkContextEmitter,
    ):
        self._activity = activity
        self._agreement = agreement
        self._storage: StorageProvider = storage
        self._emitter = emitter

        self.__payment_model: Optional[ComLinear] = None
        self.__script: Script = self.new_script()

    def emit(self, event_class: Type[ActivityEventType], **kwargs) -> ActivityEventType:
        return self._emitter(
            event_class, activity=self._activity, agreement=self._agreement, **kwargs
        )

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(id={self.id}, activity={self._activity}, provider={self.provider_id})"
        )

    @property
    def id(self) -> str:
        """Unique identifier for this work context."""
        return self._activity.id

    @property
    def provider_name(self) -> Optional[str]:
        """Return the name of the provider associated with this work context."""
        return self._agreement_details.provider_node_info.name

    @property
    def provider_id(self) -> str:
        """Return the id of the provider associated with this work context."""

        # we cannot directly make it part of the `NodeInfo` record as the `provider_id` is not
        # one of the `Offer.properties` but rather a separate attribute on the `Offer` class
        return self._agreement_details.raw_details.offer.provider_id  # type: ignore

    @property
    def _payment_model(self) -> ComLinear:
        """Return the characteristics of the payment model associated with this work context."""

        # @TODO ideally, this should return `Com` rather than `ComLinear` and the WorkContext itself
        # should be agnostic of the nature of the given payment model - but that also requires
        # automatic casting of the payment model-related properties to an appropriate model
        # inheriting from `Com`

        if not self.__payment_model:
            self.__payment_model = self._agreement_details.provider_view.extract(ComLinear)

        return self.__payment_model

    @property
    def _agreement_details(self) -> AgreementDetails:
        return self._agreement.details

    def new_script(
        self, timeout: Optional[timedelta] = None, wait_for_results: bool = True
    ) -> Script:
        """Create an instance of :class:`~yapapi.script.Script` attached to this :class:`WorkContext` instance.

        This is equivalent to calling `Script(work_context)`. This method is intended to provide a
        direct link between the two object instances.
        """
        return Script(self, timeout=timeout, wait_for_results=wait_for_results)

    async def get_raw_usage(self) -> yaa_ActivityUsage:
        """Get the raw usage vector for the activity bound to this work context.

        The value comes directly from the low level API and is not interpreted in any way.
        """
        usage = await self._activity.usage()
        logger.debug(f"WorkContext raw usage: id={self.id}, usage={usage}")
        return usage

    async def get_usage(self) -> "ActivityUsage":
        """Get the current usage for the activity bound to this work context."""
        raw_usage = await self.get_raw_usage()
        usage = ActivityUsage()
        if raw_usage.current_usage:
            usage.current_usage = self._payment_model.usage_as_dict(raw_usage.current_usage)
        if raw_usage.timestamp:
            usage.timestamp = datetime.fromtimestamp(raw_usage.timestamp, tz=get_local_timezone())
        return usage

    async def get_raw_state(self) -> yaa_ActivityState:
        """Get the state of the activity bound to this work context.

        The value comes directly from the low level API and is not interpreted in any way.
        """

        return await self._activity.state()

    async def get_cost(self) -> Optional[float]:
        """Get the accumulated cost of the activity based on the reported usage."""
        usage = await self.get_raw_usage()
        if usage.current_usage:
            return self._payment_model.calculate_cost(usage.current_usage)
        return None


class CaptureMode(enum.Enum):
    HEAD = "head"
    TAIL = "tail"
    HEAD_TAIL = "headTail"
    STREAM = "stream"


class CaptureFormat(enum.Enum):
    BIN = "bin"
    STR = "str"


@dataclass
class CaptureContext:
    mode: CaptureMode
    limit: Optional[int]
    fmt: Optional[CaptureFormat]

    @classmethod
    def build(cls, mode=None, limit=None, fmt=None) -> "CaptureContext":
        if mode in (None, "all"):
            return cls._build(CaptureMode.HEAD, fmt=fmt)
        elif mode == "stream":
            return cls._build(CaptureMode.STREAM, limit=limit, fmt=fmt)
        elif mode == "head":
            return cls._build(CaptureMode.HEAD, limit=limit, fmt=fmt)
        elif mode == "tail":
            return cls._build(CaptureMode.TAIL, limit=limit, fmt=fmt)
        elif mode == "headTail":
            return cls._build(CaptureMode.HEAD_TAIL, limit=limit, fmt=fmt)
        raise RuntimeError(f"Invalid output capture mode: {mode}")

    @classmethod
    def _build(
        cls,
        mode: CaptureMode,
        limit: Optional[int] = None,
        fmt: Optional[str] = None,
    ) -> "CaptureContext":
        cap_fmt: Optional[CaptureFormat] = CaptureFormat(fmt) if fmt else None
        return cls(mode=mode, fmt=cap_fmt, limit=limit)

    def to_dict(self) -> Dict:
        inner = dict()

        if self.limit:
            inner[self.mode.value] = self.limit
        if self.fmt:
            inner["format"] = self.fmt.value

        return {"stream" if self.mode == CaptureMode.STREAM else "atEnd": inner}

    def is_streaming(self) -> bool:
        return self.mode == CaptureMode.STREAM


@dataclass
class ActivityUsage:
    """A high-level representation of activity usage record."""

    current_usage: Dict[str, float] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
