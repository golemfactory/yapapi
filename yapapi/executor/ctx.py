import abc
import enum
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional, Dict, List, Tuple, Union

from .events import DownloadStarted, DownloadFinished
from ..props import NodeInfo
from ..storage import StorageProvider, Source, Destination


class CommandContainer:
    def __init__(self):
        self._commands = []

    def commands(self):
        return self._commands

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


class _InitStep(Work):
    def register(self, commands: CommandContainer):
        commands.deploy()
        commands.start()


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


class _SendJson(_SendWork):
    def __init__(self, storage: StorageProvider, data: dict, dst_path: str):
        super().__init__(storage, dst_path)
        self._cnt = 0
        self._data: Optional[bytes] = json.dumps(data).encode(encoding="utf-8")

    async def do_upload(self, storage: StorageProvider) -> Source:
        self._cnt += 1
        assert self._data is not None, f"json buffer unintialized {self._cnt}"
        src = await storage.upload_bytes(self._data)
        self._data = None
        return src


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


class _RecvFile(Work):
    def __init__(
        self,
        storage: StorageProvider,
        src_path: str,
        dst_path: str,
        emitter: Optional[Callable[[StorageEvent], None]] = None,
    ):
        self._storage = storage
        self._dst_path = Path(dst_path)
        self._src_path: str = src_path
        self._idx: Optional[int] = None
        self._dst_slot: Optional[Destination] = None
        self._emitter: Optional[Callable[[StorageEvent], None]] = emitter

    async def prepare(self):
        self._dst_slot = await self._storage.new_destination(destination_file=self._dst_path)

    def register(self, commands: CommandContainer):
        assert self._dst_slot, "_RecvFile command creation without prepare"

        self._idx = commands.transfer(
            _from=f"container:{self._src_path}", to=self._dst_slot.upload_url
        )

    async def post(self) -> None:
        assert self._dst_slot, "_RecvFile post without prepare"
        if self._emitter:
            self._emitter(DownloadStarted(path=self._src_path))
        await self._dst_slot.download_file(self._dst_path)
        if self._emitter:
            self._emitter(DownloadFinished(path=str(self._dst_path)))


class _Steps(Work):
    def __init__(self, *steps: Work, timeout: Optional[timedelta] = None):
        self._steps: Tuple[Work, ...] = steps
        self._timeout: Optional[timedelta] = timeout

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
    ):
        self.id = ctx_id
        self._node_info = node_info
        self._storage: StorageProvider = storage
        self._pending_steps: List[Work] = []
        self._started: bool = False
        self._emitter: Optional[Callable[[StorageEvent], None]] = emitter

    @property
    def provider_name(self) -> Optional[str]:
        """Return the name of the provider associated with this work context."""
        return self._node_info.name

    def __prepare(self):
        if not self._started:
            self._pending_steps.append(_InitStep())
            self._started = True

    def begin(self):
        pass

    def send_json(self, json_path: str, data: dict):
        """Schedule sending JSON data to the provider.

        :param json_path: remote (provider) path
        :param data: dictionary representing JSON data
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_SendJson(self._storage, data, json_path))

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
        self._pending_steps.append(_RecvFile(self._storage, src_path, dst_path, self._emitter))

    def commit(self, timeout: Optional[timedelta] = None) -> Work:
        """Creates sequence of commands to be sent to provider.

        :return: Work object (the latter contains
                 sequence commands added before calling this method)
        """
        steps = self._pending_steps
        self._pending_steps = []
        return _Steps(*steps, timeout=timeout)


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
