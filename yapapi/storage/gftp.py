"""
Golem File Transfer Storage Provider
"""

import asyncio
import contextlib
import json
import os
import sys
import tempfile
import uuid
from os import PathLike
from pathlib import Path
from types import TracebackType
from typing import List, Optional, cast, Union, AsyncIterator, Iterator, Type

import jsonrpc_base
from async_exit_stack import AsyncExitStack
from typing_extensions import Protocol, Literal, TypedDict, AsyncContextManager

from yapapi.storage import StorageProvider, Destination, Source, Content


class PubLink(TypedDict):
    file: str
    url: str


class GftpDriver(Protocol):
    async def version(self) -> str:
        pass

    async def publish(self, *, files: List[str]) -> List[PubLink]:
        pass

    async def close(self, *, urls: List[str]) -> List[Literal["ok", "error"]]:
        pass

    async def receive(self, *, output_file: str) -> PubLink:
        pass

    async def upload(self, *, file: str, url: str):
        pass

    async def shutdown(self) -> Literal["Ok", "Error"]:
        pass


def service(debug=False) -> AsyncContextManager[GftpDriver]:
    proc = __Process(_debug=debug)
    return cast(AsyncContextManager[GftpDriver], proc)


class __Process(jsonrpc_base.Server):
    def __init__(self, _debug: bool = False):
        super().__init__()
        self._debug = _debug
        self._proc: Optional[asyncio.subprocess.Process] = None

    async def __aenter__(self) -> GftpDriver:
        env = dict(os.environ, RUST_LOG="debug") if self._debug else None
        self._proc = await asyncio.create_subprocess_shell(
            "gftp server", stdout=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.PIPE, env=env
        )
        return cast(GftpDriver, self)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        with contextlib.suppress(Exception):
            await self._close()

    async def _close(self):
        if self._proc is None:
            return
        p: asyncio.subprocess.Process = self._proc
        self._proc = None
        if p.stdin:
            p.stdin.close()
            try:
                await asyncio.wait_for(p.wait(), 1.0)
                return
            except asyncio.TimeoutError:
                pass
        p.kill()
        await p.wait()

    def __log_debug(self, msg_dir: Literal["in", "out"], msg: Union[bytes, str]):
        if self._debug:
            if isinstance(msg, bytes):
                msg = msg.decode(encoding="utf-8")
            stderr = sys.stderr
            stderr.write("\n <= " if msg_dir == "in" else "\n => ")
            stderr.write(msg)
            stderr.flush()

    async def send_message(self, message):
        assert self._proc is not None
        assert self._proc.stdin is not None
        assert self._proc.stdout is not None
        bytes = message.serialize() + "\n"
        self.__log_debug("out", bytes)
        self._proc.stdin.write(bytes.encode("utf-8"))
        await self._proc.stdin.drain()
        msg = await self._proc.stdout.readline()
        self.__log_debug("in", msg)
        msg = json.loads(msg)
        return message.parse_response(msg)


@contextlib.contextmanager
def _temp_file(temp_dir: Path) -> Iterator[Path]:
    file_name = temp_dir / str(uuid.uuid4())
    yield file_name
    if file_name.exists():
        os.remove(file_name)


class GftpSource(Source):
    def __init__(self, length: int, link: PubLink):
        self._len = length
        self._link = link

    @property
    def download_url(self) -> str:
        return self._link["url"]

    async def content_length(self) -> int:
        return self._len


class GftpDestination(Destination):
    def __init__(self, _proc: GftpDriver, _link: PubLink) -> None:
        self._proc = _proc
        self._link = _link

    @property
    def upload_url(self) -> str:
        return self._link["url"]

    async def download_stream(self) -> Content:
        file_path = Path(self._link["file"])
        length = file_path.stat().st_size

        async def chunks() -> AsyncIterator[bytes]:
            with open(file_path, "rb") as f:
                chunk = f.read(30_000)
                while chunk:
                    print(f"chunk={len(chunk)}")
                    yield chunk
                    chunk = f.read(30_000)

        return Content(length=length, stream=chunks())

    async def download_file(self, destination_file: PathLike):
        if destination_file == self._link["file"]:
            print("downloaded")
            return
        return await super().download_file(destination_file)


class GftpProvider(StorageProvider, AsyncContextManager[StorageProvider]):
    _temp_dir: Optional[Path]

    def __init__(self, *, tmpdir: Optional[str] = None):
        self.__exit_stack = AsyncExitStack()
        self._temp_dir = Path(tmpdir) if tmpdir else None
        self._process = None

    async def __aenter__(self) -> StorageProvider:
        self._temp_dir = Path(self.__exit_stack.enter_context(tempfile.TemporaryDirectory()))
        process = await self.__get_process()
        _ver = await process.version()
        # TODO check version
        assert _ver
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.__exit_stack.aclose()
        return None

    def __new_file(self) -> Path:
        temp_dir: Path = self._temp_dir or Path(
            self.__exit_stack.enter_context(tempfile.TemporaryDirectory())
        )
        if not self._temp_dir:
            self._temp_dir = temp_dir
        return self.__exit_stack.enter_context(_temp_file(temp_dir))

    async def __get_process(self) -> GftpDriver:
        _debug = bool(os.getenv("DEBUG_GFTP"))
        process = self._process or (await self.__exit_stack.enter_async_context(service(_debug)))
        if not self._process:
            self._process = process
        return process

    async def upload_stream(self, length: int, stream: AsyncIterator[bytes]) -> Source:
        file_name = self.__new_file()
        with open(file_name, "wb") as f:
            async for chunk in stream:
                f.write(chunk)
        process = await self.__get_process()
        links = await process.publish(files=[str(file_name)])
        assert len(links) == 1, "invalid gftp publish response"
        link = links[0]
        return GftpSource(length, link)

    async def upload_file(self, path: os.PathLike) -> Source:
        process = await self.__get_process()
        links = await process.publish(files=[str(path)])
        length = Path(path).stat().st_size
        assert len(links) == 1, "invalid gftp publish response"
        return GftpSource(length, links[0])

    async def new_destination(self, destination_file: Optional[PathLike] = None) -> Destination:
        output_file = str(destination_file) if destination_file else str(self.__new_file())
        process = await self.__get_process()
        link = await process.receive(output_file=output_file)
        return GftpDestination(process, link)


def provider() -> AsyncContextManager[StorageProvider]:
    return GftpProvider()


__all__ = ("service", "provider")
