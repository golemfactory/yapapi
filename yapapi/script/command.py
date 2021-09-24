import abc
import asyncio
from functools import partial
import json
from os import PathLike
from pathlib import Path
from typing import Callable, List, Optional, Dict, Union, Any, Awaitable, TYPE_CHECKING


from yapapi.events import CommandExecuted, DownloadStarted, DownloadFinished
from yapapi.script.capture import CaptureContext
from yapapi.storage import StorageProvider, Source, Destination, DOWNLOAD_BYTES_LIMIT_DEFAULT


if TYPE_CHECKING:
    from yapapi.ctx import WorkContext


# For example: { "start": { "args": [] } }
BatchCommand = Dict[str, Dict[str, Union[str, List[str]]]]


class Command(abc.ABC):
    def evaluate(self, ctx: "WorkContext") -> BatchCommand:
        """Evaluate and serialize this command."""

    async def after(self, ctx: "WorkContext") -> None:
        """A hook to be executed on requestor's end after the script has finished."""
        pass

    async def before(self, ctx: "WorkContext") -> None:
        """A hook to be executed on requestor's end before the script is sent to the provider."""
        pass

    @staticmethod
    def _make_batch_command(cmd_name: str, **kwargs) -> BatchCommand:
        kwargs = dict((key[1:] if key[0] == "_" else key, value) for key, value in kwargs.items())
        return {cmd_name: kwargs}

    def __init__(self):
        self._result: asyncio.Future = asyncio.get_event_loop().create_future()


class Deploy(Command):
    """Command which deploys a given runtime on the provider."""

    def __init__(self, **kwargs: dict):
        super().__init__()
        self.kwargs = kwargs

    def __repr__(self):
        return f"deploy {self.kwargs}"

    def evaluate(self, ctx: "WorkContext"):
        return self._make_batch_command("deploy", **self.kwargs)


class Start(Command):
    """Command which starts a given runtime on the provider."""

    def __init__(self, *args: str):
        super().__init__()
        self.args = args

    def __repr__(self):
        return f"start {self.args}"

    def evaluate(self, ctx: "WorkContext"):
        return self._make_batch_command("start", args=self.args)


class Terminate(Command):
    """Command which terminates a given runtime on the provider."""

    def evaluate(self, ctx: "WorkContext"):
        return self._make_batch_command("terminate")


class _SendContent(Command, abc.ABC):
    def __init__(self, dst_path: str):
        super().__init__()
        self._dst_path = dst_path
        self._src: Optional[Source] = None

    @abc.abstractmethod
    async def _do_upload(self, storage: StorageProvider) -> Source:
        pass

    def evaluate(self, ctx: "WorkContext"):
        assert self._src
        return self._make_batch_command(
            "transfer", _from=self._src.download_url, _to=f"container:{self._dst_path}"
        )

    async def before(self, ctx: "WorkContext"):
        self._src = await self._do_upload(ctx._storage)

    async def after(self, ctx: "WorkContext") -> None:
        assert self._src is not None
        await ctx._storage.release_source(self._src)


class SendBytes(_SendContent):
    """Command which schedules sending bytes data to a provider."""

    def __init__(self, data: bytes, dst_path: str):
        """Create a new SendBytes command.

        :param data: bytes to send
        :param dst_path: remote (provider) destination path
        """
        super().__init__(dst_path)
        self._data: Optional[bytes] = data

    async def _do_upload(self, storage: StorageProvider) -> Source:
        assert self._data is not None, f"{self}: buffer unintialized"
        src = await storage.upload_bytes(self._data)
        self._data = None
        return src


class SendJson(SendBytes):
    """Command which schedules sending JSON data to a provider."""

    def __init__(self, data: dict, dst_path: str):
        """Create a new SendJson command.

        :param data: dictionary representing JSON data to send
        :param dst_path: remote (provider) destination path
        """
        super().__init__(json.dumps(data).encode(encoding="utf-8"), dst_path)


class SendFile(_SendContent):
    """Command which schedules sending a file to a provider."""

    def __init__(self, src_path: str, dst_path: str):
        """Create a new SendFile command.

        :param src_path: local (requestor) source path
        :param dst_path: remote (provider) destination path
        """
        super(SendFile, self).__init__(dst_path)
        self._src_path = Path(src_path)

    async def _do_upload(self, storage: StorageProvider) -> Source:
        return await storage.upload_file(self._src_path)


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

    def evaluate(self, ctx: "WorkContext"):
        capture = {"stdout": self.stdout.to_dict(), "stderr": self.stderr.to_dict()}
        return self._make_batch_command(
            "run", entry_point=self.cmd, args=self.args, capture=capture
        )


StorageEvent = Union[DownloadStarted, DownloadFinished]


class _ReceiveContent(Command, abc.ABC):
    def __init__(
        self,
        src_path: str,
    ):
        super().__init__()
        self._src_path: str = src_path
        self._dst_slot: Optional[Destination] = None
        self._dst_path: Optional[PathLike] = None

    def evaluate(self, ctx: "WorkContext"):
        assert self._dst_slot
        return self._make_batch_command(
            "transfer", _from=f"container:{self._src_path}", to=self._dst_slot.upload_url
        )

    async def before(self, ctx: "WorkContext"):
        self._dst_slot = await ctx._storage.new_destination(destination_file=self._dst_path)

    def _emit_download_start(self, ctx: "WorkContext"):
        assert self._dst_slot, f"{self.__class__} after without before"
        if ctx._emitter:
            ctx._emitter(DownloadStarted(path=self._src_path))

    def _emit_download_end(self, ctx: "WorkContext"):
        if ctx._emitter:
            ctx._emitter(DownloadFinished(path=str(self._dst_path)))


class DownloadFile(_ReceiveContent):
    """Command which schedules downloading a file from a provider."""

    def __init__(
        self,
        src_path: str,
        dst_path: str,
    ):
        """Create a new DownloadFile command.

        :param src_path: remote (provider) source path
        :param dst_path: local (requestor) destination path
        """
        super().__init__(src_path)
        self._dst_path = Path(dst_path)

    async def after(self, ctx: "WorkContext") -> None:
        self._emit_download_start(ctx)
        assert self._dst_path
        assert self._dst_slot

        await self._dst_slot.download_file(self._dst_path)
        self._emit_download_end(ctx)


class DownloadBytes(_ReceiveContent):
    """Command which schedules downloading a file from a provider as bytes."""

    def __init__(
        self,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ):
        """Create a new DownloadBytes command.

        :param src_path: remote (provider) source path
        :param on_download: the callable to run on the received data
        :param limit: limit of bytes to be downloaded (expected size)
        """
        super().__init__(src_path)
        self._on_download = on_download
        self._limit = limit

    async def after(self, ctx: "WorkContext") -> None:
        self._emit_download_start(ctx)
        assert self._dst_slot

        output = await self._dst_slot.download_bytes(limit=self._limit)
        self._emit_download_end(ctx)
        await self._on_download(output)


class DownloadJson(DownloadBytes):
    """Command which schedules downloading a file from a provider as JSON data."""

    def __init__(
        self,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ):
        """Create a new DownloadJson command.

        :param src_path: remote (provider) source path
        :param on_download: the callable to run on the received data
        :param limit: limit of bytes to be downloaded (expected size)
        """
        super().__init__(src_path, partial(self.__on_json_download, on_download), limit)

    @staticmethod
    async def __on_json_download(on_download: Callable[[bytes], Awaitable], content: bytes):
        await on_download(json.loads(content))
