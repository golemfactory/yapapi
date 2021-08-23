"""Stuff."""

import abc
from datetime import timedelta
from functools import partial
import json
from os import PathLike
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Dict, Union, Any, Awaitable


from yapapi.ctx import WorkContext
from yapapi.events import DownloadStarted, DownloadFinished
from yapapi.script.capture import CaptureContext
from yapapi.storage import StorageProvider, Source, Destination, DOWNLOAD_BYTES_LIMIT_DEFAULT


# For example: { "start": { "args": [] } }
BatchCommand = Dict[str, Dict[str, Union[str, List[str]]]]


class Work(abc.ABC):
    async def evaluate(self, ctx: WorkContext) -> BatchCommand:
        """Stuff."""

    async def prepare(self, ctx: WorkContext) -> None:
        """A hook to be executed on requestor's end before the script is sent to the provider."""
        pass

    async def post(self, ctx: WorkContext) -> None:
        """A hook to be executed on requestor's end after the script has finished."""
        pass

    @staticmethod
    def _make_command(cmd_name: str, **kwargs) -> BatchCommand:
        kwargs = dict((key[1:] if key[0] == "_" else key, value) for key, value in kwargs.items())
        return {cmd_name: kwargs}


class _Deploy(Work):
    async def evaluate(self, ctx: WorkContext):
        return self._make_command("deploy")


class _Start(Work):
    def __init__(self, *args: str):
        self.args = args

    def __repr__(self):
        return f"start{self.args}"

    async def evaluate(self, ctx: WorkContext):
        return self._make_command("start", args=self.args)


class _Terminate(Work):
    async def evaluate(self, ctx: WorkContext):
        return self._make_command("terminate")


class _SendWork(Work, abc.ABC):
    def __init__(self, dst_path: str):
        self._dst_path = dst_path
        self._src: Optional[Source] = None
        self._idx: Optional[int] = None

    @abc.abstractmethod
    async def do_upload(self, storage: StorageProvider) -> Source:
        pass

    async def evaluate(self, ctx: WorkContext):
        self._src = await self.do_upload(ctx._storage)
        assert self._src is not None, "cmd prepared"
        return self._make_command(
            "transfer", _from=self._src.download_url, _to=f"container:{self._dst_path}"
        )

    async def prepare(self, ctx: WorkContext):
        self._src = await self.do_upload(ctx._storage)

    async def post(self, ctx: WorkContext) -> None:
        assert self._src is not None
        await ctx._storage.release_source(self._src)


class _SendBytes(_SendWork):
    def __init__(self, data: bytes, dst_path: str):
        super().__init__(dst_path)
        self._data: Optional[bytes] = data

    async def do_upload(self, storage: StorageProvider) -> Source:
        assert self._data is not None, "buffer unintialized"
        src = await storage.upload_bytes(self._data)
        self._data = None
        return src


class _SendJson(_SendBytes):
    def __init__(self, data: dict, dst_path: str):
        super().__init__(json.dumps(data).encode(encoding="utf-8"), dst_path)


class _SendFile(_SendWork):
    def __init__(self, src_path: str, dst_path: str):
        super(_SendFile, self).__init__(dst_path)
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

    def evaluate(self, ctx: WorkContext):
        capture = dict()
        if self.stdout:
            capture["stdout"] = self.stdout.to_dict()
        if self.stderr:
            capture["stderr"] = self.stderr.to_dict()
        return self._make_command("run", entry_point=self.cmd, args=self.args, capture=capture)


StorageEvent = Union[DownloadStarted, DownloadFinished]


class _ReceiveContent(Work):
    def __init__(
        self,
        src_path: str,
    ):
        self._src_path: str = src_path
        self._idx: Optional[int] = None
        self._dst_slot: Optional[Destination] = None
        self._dst_path: Optional[PathLike] = None

    async def evaluate(self, ctx: WorkContext):
        dst_slot = await ctx._storage.new_destination(destination_file=self._dst_path)
        return self._make_command(
            "transfer", _from=f"container:{self._src_path}", to=dst_slot.upload_url
        )

    async def prepare(self, ctx: WorkContext):
        self._dst_slot = await self._storage.new_destination(destination_file=self._dst_path)

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
        src_path: str,
        dst_path: str,
    ):
        super().__init__(src_path)
        self._dst_path = Path(dst_path)

    async def post(self, ctx: WorkContext) -> None:
        self._emit_download_start()
        assert self._dst_path
        assert self._dst_slot

        await self._dst_slot.download_file(self._dst_path)
        self._emit_download_end()


class _ReceiveBytes(_ReceiveContent):
    def __init__(
        self,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ):
        super().__init__(src_path)
        self._on_download = on_download
        self._limit = limit

    async def post(self, ctx: WorkContext) -> None:
        self._emit_download_start()
        assert self._dst_slot

        output = await self._dst_slot.download_bytes(limit=self._limit)
        self._emit_download_end()
        await self._on_download(output)


class _ReceiveJson(_ReceiveBytes):
    def __init__(
        self,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
    ):
        super().__init__(src_path, partial(self.__on_json_download, on_download), limit)

    @staticmethod
    async def __on_json_download(on_download: Callable[[bytes], Awaitable], content: bytes):
        await on_download(json.loads(content))
