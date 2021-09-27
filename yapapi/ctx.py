import abc
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from deprecated import deprecated  # type: ignore
import enum
import logging
from typing import Callable, Optional, Dict, List, Any, Awaitable

from ya_activity.models import (
    ActivityUsage as yaa_ActivityUsage,
    ActivityState as yaa_ActivityState,
)

from yapapi.events import CommandExecuted
from yapapi.props.com import ComLinear
from yapapi.script import Script
from yapapi.storage import StorageProvider, DOWNLOAD_BYTES_LIMIT_DEFAULT
from yapapi.rest.market import AgreementDetails
from yapapi.rest.activity import Activity
from yapapi.script.command import StorageEvent
from yapapi.utils import get_local_timezone

logger = logging.getLogger(__name__)


@deprecated(version="0.7.0", reason="replaced by Script._commands")
class CommandContainer:
    def __init__(self):
        self._commands = []

    def commands(self):
        return self._commands

    def __repr__(self):
        return f"commands: {self._commands}"

    def __getattr__(self, item):
        def add_command(**kwargs) -> int:
            kwargs = dict(
                (key[1:] if key[0] == "_" else key, value) for key, value in kwargs.items()
            )
            idx = len(self._commands)
            self._commands.append({item: kwargs})
            return idx

        return add_command


class Work(abc.ABC):
    async def prepare(self):
        """A hook to be executed on requestor's end before the script is sent to the provider."""
        pass

    def register(self, commands: CommandContainer):
        """A hook which adds the required command to the exescript."""
        pass

    async def post(self):
        """A hook to be executed on requestor's end after the script has finished."""
        pass

    @property
    def timeout(self) -> Optional[timedelta]:
        """Return the optional timeout set for execution of this work."""
        return None


@dataclass
class ExecOptions:
    """Options related to command batch execution."""

    wait_for_results: bool = True
    batch_timeout: Optional[timedelta] = None


class WorkContext:
    """Provider node's work context.

    Used to schedule commands to be sent to the provider and enable other interactions between the
    requestor agent's client code and the activity on provider's end.
    """

    def __init__(
        self,
        activity: Activity,
        agreement_details: AgreementDetails,
        storage: StorageProvider,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
    ):
        self._activity = activity
        self._agreement_details = agreement_details
        self._storage: StorageProvider = storage
        self._emitter: Optional[Callable[[StorageEvent], None]] = emitter

        self._pending_steps: List[Work] = []

        self.__payment_model: Optional[ComLinear] = None
        self.__script: Script = self.new_script()

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

    def new_script(
        self, timeout: Optional[timedelta] = None, wait_for_results: bool = True
    ) -> Script:
        """Create an instance of :class:`~yapapi.script.Script` attached to this :class:`WorkContext` instance.

        This is equivalent to calling `Script(work_context)`. This method is intended to provide a
        direct link between the two object instances.
        """
        return Script(self, timeout=timeout, wait_for_results=wait_for_results)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def deploy(self, **kwargs) -> Awaitable[CommandExecuted]:
        """Schedule a Deploy command."""
        return self.__script.deploy(**kwargs)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def start(self, *args: str) -> Awaitable[CommandExecuted]:
        """Schedule a Start command."""
        return self.__script.start(*args)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def terminate(self) -> Awaitable[CommandExecuted]:
        """Schedule a Terminate command."""
        return self.__script.terminate()

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def send_json(self, json_path: str, data: dict) -> Awaitable[CommandExecuted]:
        """Schedule sending JSON data to the provider.

        :param json_path: remote (provider) path
        :param data: dictionary representing JSON data
        :return: None
        """
        return self.__script.upload_json(data, json_path)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def send_bytes(self, dst_path: str, data: bytes) -> Awaitable[CommandExecuted]:
        """Schedule sending bytes data to the provider.

        :param dst_path: remote (provider) path
        :param data: bytes to send
        :return: None
        """
        return self.__script.upload_bytes(data, dst_path)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def send_file(self, src_path: str, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending file to the provider.

        :param src_path: local (requestor) path
        :param dst_path: remote (provider) path
        :return: None
        """
        return self.__script.upload_file(src_path, dst_path)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def run(
        self,
        cmd: str,
        *args: str,
        env: Optional[Dict[str, str]] = None,
    ) -> Awaitable[CommandExecuted]:
        """Schedule running a command.

        :param cmd: command to run on the provider, e.g. /my/dir/run.sh
        :param args: command arguments, e.g. "input1.txt", "output1.txt"
        :param env: optional dictionary with environmental variables
        """
        return self.__script.run(cmd, *args, env=env)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def download_file(self, src_path: str, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule downloading remote file from the provider.

        :param src_path: remote (provider) path
        :param dst_path: local (requestor) path
        """
        return self.__script.download_file(src_path, dst_path)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def download_bytes(
        self,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ) -> Awaitable[CommandExecuted]:
        """Schedule downloading a remote file as bytes

        :param src_path: remote (provider) path
        :param on_download: the callable to run on the received data
        :param limit: the maximum length of the expected byte string
        """
        return self.__script.download_bytes(src_path, on_download, limit)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def download_json(
        self,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ) -> Awaitable[CommandExecuted]:
        """Schedule downloading a remote file as JSON

        :param src_path: remote (provider) path
        :param on_download: the callable to run on the received JSON data
        :param limit: the maximum length of the expected remote file
        """
        return self.__script.download_json(src_path, on_download, limit)

    @deprecated(version="0.7.0", reason="please use a Script object via WorkContext.new_script")
    def commit(self, timeout: Optional[timedelta] = None) -> Script:
        """Creates a sequence of commands to be sent to provider.

        :return: Script object containing the sequence of commands
                 scheduled within this work context before calling this method
        """
        assert self.__script._commands, "commit called with no commands scheduled"
        if timeout:
            self.__script.timeout = timeout
        script_to_commit = self.__script
        self.__script = self.new_script()
        return script_to_commit

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
        """Get the state activity bound to this work context.

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
