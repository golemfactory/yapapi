import abc
import enum
import json
from dataclasses import dataclass
from datetime import timedelta
from os import PathLike
from functools import partial
from pathlib import Path
from typing import Callable, Iterable, Optional, Dict, List, Tuple, Union, Any, Awaitable

from yapapi.events import DownloadStarted, DownloadFinished
from yapapi.props import NodeInfo
from yapapi.storage import StorageProvider, Source, Destination, DOWNLOAD_BYTES_LIMIT_DEFAULT


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


class _Deploy(Work):
    def register(self, commands: CommandContainer):
        commands.deploy()


class _Start(Work):
    def __init__(self, *args: str):
        self.args = args

    def __repr__(self):
        return f"start{self.args}"

    def register(self, commands: CommandContainer):
        commands.start(args=self.args)


class _Terminate(Work):
    def register(self, commands: CommandContainer):
        commands.terminate()


class _SendWork(Work, abc.ABC):
    def __init__(self, storage: StorageProvider, dst_path: str):
        self._storage = storage
        self._dst_path = dst_path
        self._src: Optional[Source] = None
        self._idx: Optional[int] = None

    @abc.abstractmethod
    async def do_upload(self, storage: StorageProvider) -> Source:
        pass

    async def prepare(self):
        self._src = await self.do_upload(self._storage)

    def register(self, commands: CommandContainer):
        assert self._src is not None, "cmd prepared"
        self._idx = commands.transfer(
            _from=self._src.download_url, _to=f"container:{self._dst_path}"
        )

    async def post(self) -> None:
        assert self._src is not None
        await self._storage.release_source(self._src)


class _SendBytes(_SendWork):
    def __init__(self, storage: StorageProvider, data: bytes, dst_path: str):
        super().__init__(storage, dst_path)
        self._data: Optional[bytes] = data

    async def do_upload(self, storage: StorageProvider) -> Source:
        assert self._data is not None, "buffer unintialized"
        src = await storage.upload_bytes(self._data)
        self._data = None
        return src


class _SendJson(_SendBytes):
    def __init__(self, storage: StorageProvider, data: dict, dst_path: str):
        super().__init__(storage, json.dumps(data).encode(encoding="utf-8"), dst_path)


class _SendFile(_SendWork):
    def __init__(self, storage: StorageProvider, src_path: str, dst_path: str):
        super(_SendFile, self).__init__(storage, dst_path)
        self._src_path = Path(src_path)

    async def do_upload(self, storage: StorageProvider) -> Source:
        return await storage.upload_file(self._src_path)


class _Run(Work):
    def __init__(
        self,
        cmd: str,
        *args: Iterable[str],
        env: Optional[Dict[str, str]] = None,
        stdout: Optional["CaptureContext"] = None,
        stderr: Optional["CaptureContext"] = None,
    ):
        self.cmd = cmd
        self.args = args
        self.env = env
        self.stdout = stdout
        self.stderr = stderr
        self._idx = None

    def register(self, commands: CommandContainer):
        capture = dict()
        if self.stdout:
            capture["stdout"] = self.stdout.to_dict()
        if self.stderr:
            capture["stderr"] = self.stderr.to_dict()
        self._idx = commands.run(entry_point=self.cmd, args=self.args, capture=capture)


StorageEvent = Union[DownloadStarted, DownloadFinished]


class _ReceiveContent(Work):
    def __init__(
        self,
        storage: StorageProvider,
        src_path: str,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
    ):
        self._storage = storage
        self._src_path: str = src_path
        self._idx: Optional[int] = None
        self._dst_slot: Optional[Destination] = None
        self._emitter: Optional[Callable[[StorageEvent], None]] = emitter
        self._dst_path: Optional[PathLike] = None

    async def prepare(self):
        self._dst_slot = await self._storage.new_destination(destination_file=self._dst_path)

    def register(self, commands: CommandContainer):
        assert self._dst_slot, f"{self.__class__} command creation without prepare"

        self._idx = commands.transfer(
            _from=f"container:{self._src_path}", to=self._dst_slot.upload_url
        )

    def _emit_download_start(self):
        assert self._dst_slot, f"{self.__class__} post without prepare"
        if self._emitter:
            self._emitter(DownloadStarted(path=self._src_path))

    def _emit_download_end(self):
        if self._emitter:
            self._emitter(DownloadFinished(path=str(self._dst_path)))


class _ReceiveFile(_ReceiveContent):
    def __init__(
        self,
        storage: StorageProvider,
        src_path: str,
        dst_path: str,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
    ):
        super().__init__(storage, src_path, emitter)
        self._dst_path = Path(dst_path)

    async def post(self) -> None:
        self._emit_download_start()
        assert self._dst_path
        assert self._dst_slot

        await self._dst_slot.download_file(self._dst_path)
        self._emit_download_end()


class _ReceiveBytes(_ReceiveContent):
    def __init__(
        self,
        storage: StorageProvider,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
    ):
        super().__init__(storage, src_path, emitter)
        self._on_download = on_download
        self._limit = limit

    async def post(self) -> None:
        self._emit_download_start()
        assert self._dst_slot

        output = await self._dst_slot.download_bytes(limit=self._limit)
        self._emit_download_end()
        await self._on_download(output)


class _ReceiveJson(_ReceiveBytes):
    def __init__(
        self,
        storage: StorageProvider,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
    ):
        super().__init__(
            storage, src_path, partial(self.__on_json_download, on_download), limit, emitter
        )

    @staticmethod
    async def __on_json_download(on_download: Callable[[bytes], Awaitable], content: bytes):
        await on_download(json.loads(content))


class Steps(Work):
    def __init__(self, *steps: Work, timeout: Optional[timedelta] = None):
        """Create a `Work` item consisting of a sequence of steps (subitems).

        :param steps: sequence of steps to be executed
        :param timeout: timeout for waiting for the steps' results
        """
        self._steps: Tuple[Work, ...] = steps
        self._timeout: Optional[timedelta] = timeout

    def __repr__(self):
        return f"{self.__class__.__name__}: {self._steps}"

    @property
    def timeout(self) -> Optional[timedelta]:
        """Return the optional timeout set for execution of all steps."""
        return self._timeout

    async def prepare(self):
        """Execute the `prepare` hook for all the defined steps."""
        for step in self._steps:
            await step.prepare()

    def register(self, commands: CommandContainer):
        """Execute the `register` hook for all the defined steps."""
        for step in self._steps:
            step.register(commands)

    async def post(self):
        """Execute the `post` step for all the defined steps."""
        for step in self._steps:
            await step.post()


@dataclass
class ExecOptions:
    """Options related to command batch execution."""

    wait_for_results: bool = True
    batch_timeout: Optional[timedelta] = None


class WorkContext:
    """An object used to schedule commands to be sent to provider."""

    id: str
    """Unique identifier for this work context."""

    def __init__(
        self,
        ctx_id: str,
        node_info: NodeInfo,
        storage: StorageProvider,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
        implicit_init: bool = True,
    ):
        self.id = ctx_id
        self._node_info = node_info
        self._storage: StorageProvider = storage
        self._pending_steps: List[Work] = []
        self._started: bool = False
        self._emitter: Optional[Callable[[StorageEvent], None]] = emitter
        self._implicit_init = implicit_init

    @property
    def provider_name(self) -> Optional[str]:
        """Return the name of the provider associated with this work context."""
        return self._node_info.name

    def __prepare(self):
        if not self._started and self._implicit_init:
            self.deploy()
            self.start()
            self._started = True

    def begin(self):
        pass

    def deploy(self):
        """Schedule a Deploy command."""
        self._implicit_init = False
        self._pending_steps.append(_Deploy())

    def start(self, *args: str):
        """Schedule a Start command."""
        self._implicit_init = False
        self._pending_steps.append(_Start(*args))

    def terminate(self):
        """Schedule a Terminate command."""
        self._pending_steps.append(_Terminate())

    def send_json(self, json_path: str, data: dict):
        """Schedule sending JSON data to the provider.

        :param json_path: remote (provider) path
        :param data: dictionary representing JSON data
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_SendJson(self._storage, data, json_path))

    def send_bytes(self, dst_path: str, data: bytes):
        """Schedule sending bytes data to the provider.

        :param dst_path: remote (provider) path
        :param data: bytes to send
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_SendBytes(self._storage, data, dst_path))

    def send_file(self, src_path: str, dst_path: str):
        """Schedule sending file to the provider.

        :param src_path: local (requestor) path
        :param dst_path: remote (provider) path
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_SendFile(self._storage, src_path, dst_path))

    def run(
        self,
        cmd: str,
        *args: Iterable[str],
        env: Optional[Dict[str, str]] = None,
    ):
        """Schedule running a command.

        :param cmd: command to run on the provider, e.g. /my/dir/run.sh
        :param args: command arguments, e.g. "input1.txt", "output1.txt"
        :param env: optional dictionary with environmental variables
        :return: None
        """
        stdout = CaptureContext.build(mode="stream")
        stderr = CaptureContext.build(mode="stream")

        self.__prepare()
        self._pending_steps.append(_Run(cmd, *args, env=env, stdout=stdout, stderr=stderr))

    def download_file(self, src_path: str, dst_path: str):
        """Schedule downloading remote file from the provider.

        :param src_path: remote (provider) path
        :param dst_path: local (requestor) path
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_ReceiveFile(self._storage, src_path, dst_path, self._emitter))

    def download_bytes(
        self,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ):
        """Schedule downloading a remote file as bytes
        :param src_path: remote (provider) path
        :param on_download: the callable to run on the received data
        :param limit: the maximum length of the expected byte string
        :return None
        """
        self.__prepare()
        self._pending_steps.append(
            _ReceiveBytes(self._storage, src_path, on_download, limit, self._emitter)
        )

    def download_json(
        self,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ):
        """Schedule downloading a remote file as JSON
        :param src_path: remote (provider) path
        :param on_download: the callable to run on the received JSON data
        :param limit: the maximum length of the expected remote file
        :return None
        """
        self.__prepare()
        self._pending_steps.append(
            _ReceiveJson(self._storage, src_path, on_download, limit, self._emitter)
        )

    def commit(self, timeout: Optional[timedelta] = None) -> Work:
        """Creates a sequence of commands to be sent to provider.

        :return: Work object containing the sequence of commands
                 scheduled within this work context before calling this method)
        """
        steps = self._pending_steps
        self._pending_steps = []
        return Steps(*steps, timeout=timeout)


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
