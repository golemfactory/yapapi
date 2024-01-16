import abc
import asyncio
import json
from functools import partial
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional, Type, Union

import attr

from yapapi.events import CommandEventType, DownloadFinished, DownloadStarted
from yapapi.script.capture import CaptureContext
from yapapi.storage import DOWNLOAD_BYTES_LIMIT_DEFAULT, Destination, Source, StorageProvider

if TYPE_CHECKING:
    from yapapi.script import Script

# For example: { "start": { "args": [] } }
BatchCommand = Dict[str, Dict[str, Union[str, List[str]]]]


class Command(abc.ABC):
    @abc.abstractmethod
    def evaluate(self) -> BatchCommand:
        """Evaluate and serialize this command."""

    async def after(self) -> None:
        """Execute a hook to be executed on requestor's end after the script has finished."""

    async def before(self) -> None:
        """Execute a hook to be executed on requestor's end before the script is sent to the \
        provider."""

    @staticmethod
    def _make_batch_command(cmd_name: str, **kwargs) -> BatchCommand:
        kwargs = dict((key[1:] if key[0] == "_" else key, value) for key, value in kwargs.items())
        return {cmd_name: kwargs}

    def __init__(self):
        self._result: asyncio.Future = asyncio.get_event_loop().create_future()
        self._script: Optional["Script"] = None
        self._index: int = 0

    def _set_script(self, script: "Script", index: int) -> None:
        assert self._script is None, f"Command {self} already belongs to a script {self._script}"
        self._script = script
        self._index = index

    def emit(self, event_class: Type[CommandEventType], **kwargs) -> CommandEventType:
        if self._script is None:
            raise RuntimeError("Only commands attached to a Script can emit")
        return self._script.emit(event_class, command=self, **kwargs)

    @property
    def _storage(self) -> StorageProvider:
        assert self._script is not None, "_storage is known only for commands in a Script"
        return self._script._ctx._storage

    def __repr__(self):
        return f"{self.__class__.__name__}"


@attr.s(auto_attribs=True, repr=False)
class ProgressArgs:
    """Interval represented as human-readable duration string (examples: '5s' '10min')."""

    updateInterval: Optional[str] = attr.field(default=None)
    updateStep: Optional[int] = attr.field(default=None)


class Deploy(Command):
    """Command which deploys a given runtime on the provider."""

    def __init__(self, progress_args: Optional[ProgressArgs] = None, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        self._progress = progress_args

    def __repr__(self):
        return f"{super().__repr__()} {self.kwargs}"

    def evaluate(self):
        kwargs = (
            dict(self.kwargs, progress=attr.asdict(self._progress))
            if self._progress
            else self.kwargs
        )
        return self._make_batch_command("deploy", **kwargs)


class Start(Command):
    """Command which starts a given runtime on the provider."""

    def __init__(self, *args: str):
        super().__init__()
        self.args = args

    def __repr__(self):
        return f"{super().__repr__()} {self.args}"

    def evaluate(self):
        return self._make_batch_command("start", args=self.args)


class Terminate(Command):
    """Command which terminates a given runtime on the provider."""

    def evaluate(self):
        return self._make_batch_command("terminate")


class _SendContent(Command, abc.ABC):
    def __init__(self, dst_path: str, progress_args: Optional[ProgressArgs] = None):
        super().__init__()
        self._dst_path = dst_path
        self._src: Optional[Source] = None
        self._progress = progress_args

    @abc.abstractmethod
    async def _do_upload(self, storage: StorageProvider) -> Source:
        pass

    def evaluate(self):
        assert self._src

        kwargs = {"progress": attr.asdict(self._progress)} if self._progress else {}
        return self._make_batch_command(
            "transfer", _from=self._src.download_url, _to=f"container:{self._dst_path}", **kwargs
        )

    async def before(self):
        self._src = await self._do_upload(self._storage)

    async def after(self) -> None:
        assert self._src is not None
        await self._storage.release_source(self._src)

    def __repr__(self):
        return f"{super().__repr__()} dst={self._dst_path}"


class SendBytes(_SendContent):
    """Command which schedules sending bytes data to a provider."""

    def __init__(self, data: bytes, dst_path: str, progress_args: Optional[ProgressArgs] = None):
        """Create a new SendBytes command.

        :param data: bytes to send
        :param dst_path: remote (provider) destination path
        """
        super().__init__(dst_path, progress_args=progress_args)
        self._data: Optional[bytes] = data

    async def _do_upload(self, storage: StorageProvider) -> Source:
        assert self._data is not None, f"{self}: buffer unintialized"
        src = await storage.upload_bytes(self._data)
        self._data = None
        return src


class SendJson(SendBytes):
    """Command which schedules sending JSON data to a provider."""

    def __init__(self, data: dict, dst_path: str, progress_args: Optional[ProgressArgs] = None):
        """Create a new SendJson command.

        :param data: dictionary representing JSON data to send
        :param dst_path: remote (provider) destination path
        """
        super().__init__(
            json.dumps(data).encode(encoding="utf-8"), dst_path, progress_args=progress_args
        )


class SendFile(_SendContent):
    """Command which schedules sending a file to a provider."""

    def __init__(self, src_path: str, dst_path: str, progress_args: Optional[ProgressArgs] = None):
        """Create a new SendFile command.

        :param src_path: local (requestor) source path
        :param dst_path: remote (provider) destination path
        """
        super(SendFile, self).__init__(dst_path, progress_args=progress_args)
        self._src_path = Path(src_path)

    async def _do_upload(self, storage: StorageProvider) -> Source:
        return await storage.upload_file(self._src_path)

    def __repr__(self):
        return f"{super().__repr__()} src={self._src_path}"


class Run(Command):
    """Command which schedules running a shell command on a provider."""

    def __init__(
        self,
        cmd: str,
        *args: str,
        env: Optional[Dict[str, str]] = None,
        stderr: CaptureContext = CaptureContext.build(mode="stream"),
        stdout: CaptureContext = CaptureContext.build(mode="stream"),
    ):
        """Create a new Run command.

        :param cmd: command to run on the provider, e.g. /my/dir/run.sh
        :param args: command arguments, e.g. "input1.txt", "output1.txt"
        :param env: optional dictionary with environment variables
        :param stderr: capture context to use for stderr
        :param stdout: capture context to use for stdout
        """
        super().__init__()
        self.cmd = cmd
        self.args = args
        self.env = env
        self.stderr = stderr
        self.stdout = stdout

    def evaluate(self):
        capture = {"stdout": self.stdout.to_dict(), "stderr": self.stderr.to_dict()}
        return self._make_batch_command(
            "run", entry_point=self.cmd, args=self.args, capture=capture
        )

    def __repr__(self):
        return f"{super().__repr__()} {self.cmd} {self.args}"


StorageEvent = Union[DownloadStarted, DownloadFinished]


class _ReceiveContent(Command, abc.ABC):
    def __init__(self, src_path: str, progress_args: Optional[ProgressArgs] = None):
        super().__init__()
        self._src_path: str = src_path
        self._dst_slot: Optional[Destination] = None
        self._dst_path: Optional[PathLike] = None
        self._progress = progress_args

    def evaluate(self):
        assert self._dst_slot

        kwargs = {"progress": attr.asdict(self._progress)} if self._progress else {}
        return self._make_batch_command(
            "transfer", _from=f"container:{self._src_path}", to=self._dst_slot.upload_url, **kwargs
        )

    async def before(self):
        self._dst_slot = await self._storage.new_destination(destination_file=self._dst_path)

    def _emit_download_start(self):
        assert self._dst_slot, f"{self.__class__} after without before"
        self.emit(DownloadStarted)

    def _emit_download_end(self):
        self.emit(DownloadFinished)

    def __repr__(self):
        return f"{super().__repr__()} src={self._src_path}"


class DownloadFile(_ReceiveContent):
    """Command which schedules downloading a file from a provider."""

    def __init__(
        self,
        src_path: str,
        dst_path: str,
        progress_args: Optional[ProgressArgs] = None,
    ):
        """Create a new DownloadFile command.

        :param src_path: remote (provider) source path
        :param dst_path: local (requestor) destination path
        """
        super().__init__(src_path, progress_args=progress_args)
        self._dst_path = Path(dst_path)

    async def after(self) -> None:
        self._emit_download_start()
        assert self._dst_path
        assert self._dst_slot

        await self._dst_slot.download_file(self._dst_path)
        self._emit_download_end()

    def __repr__(self):
        return f"{super().__repr__()} dst={self._dst_path}"


class DownloadBytes(_ReceiveContent):
    """Command which schedules downloading a file from a provider as bytes."""

    def __init__(
        self,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
        progress_args: Optional[ProgressArgs] = None,
    ):
        """Create a new DownloadBytes command.

        :param src_path: remote (provider) source path
        :param on_download: the callable to run on the received data
        :param limit: limit of bytes to be downloaded (expected size)
        """
        super().__init__(src_path, progress_args=progress_args)
        self._on_download = on_download
        self._limit = limit

    async def after(self) -> None:
        self._emit_download_start()
        assert self._dst_slot

        output = await self._dst_slot.download_bytes(limit=self._limit)
        self._emit_download_end()
        await self._on_download(output)


class DownloadJson(DownloadBytes):
    """Command which schedules downloading a file from a provider as JSON data."""

    def __init__(
        self,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
        progress_args: Optional[ProgressArgs] = None,
    ):
        """Create a new DownloadJson command.

        :param src_path: remote (provider) source path
        :param on_download: the callable to run on the received data
        :param limit: limit of bytes to be downloaded (expected size)
        """
        super().__init__(
            src_path,
            partial(self.__on_json_download, on_download),
            limit,
            progress_args=progress_args,
        )

    @staticmethod
    async def __on_json_download(on_download: Callable[[bytes], Awaitable], content: bytes):
        await on_download(json.loads(content))


class InternetSource(Source):
    def __init__(self, url: str):
        self._url = url

    @property
    def download_url(self) -> str:
        return self._url

    async def content_length(self) -> int:
        return 0


class DownloadFileFromInternet(_SendContent):
    def __init__(self, src_url: str, dst_path: str, progress_args: Optional[ProgressArgs] = None):
        """Create a new UploadFileFromInternet command.

        :param src_url: remote (internet) source url
        :param dst_path: remote (provider) destination path
        """
        super().__init__(dst_path, progress_args=progress_args)
        self._src_url = src_url

    async def _do_upload(self, storage: StorageProvider) -> Source:
        return InternetSource(self._src_url)

    async def after(self) -> None:
        pass

    def __repr__(self):
        return f"{super().__repr__()} src={self._src_url}"
