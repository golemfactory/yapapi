"""
Golem File Transfer Storage Provider
"""

import asyncio
import contextlib
from dataclasses import dataclass
import distutils.util
import json
import os
import sys
import tempfile
from os import PathLike
from pathlib import Path
from types import TracebackType
from typing import (
    AsyncContextManager,
    AsyncIterator,
    cast,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Type,
    Union,
)

import jsonrpc_base  # type: ignore
from async_exit_stack import AsyncExitStack  # type: ignore
import semantic_version  # type: ignore
from typing_extensions import Literal, Protocol, TypedDict

from yapapi.storage import StorageProvider, Destination, Source, Content
import logging


_logger = logging.getLogger(__name__)


class PubLink(TypedDict):
    """GFTP linking information."""

    file: str
    """file on local filesystem."""

    url: str
    """GFTP url as which local files is exposed."""


CommandStatus = Literal["ok", "error"]


class GftpDriver(Protocol):
    """Golem FTP service API."""

    async def version(self) -> str:
        """Gets driver version."""
        pass

    async def publish(self, *, files: List[str]) -> List[PubLink]:
        """Exposes local file as GFTP url.

        `files`
        :   local files to be exposed

        """
        pass

    async def close(self, *, urls: List[str]) -> List[CommandStatus]:
        """Stops exposing GFTP urls created by [publish(files=[..])](#publish)."""
        pass

    async def receive(self, *, output_file: str) -> PubLink:
        """Creates GFTP url for receiving file.

        :  `output_file` -
        """
        pass

    async def upload(self, *, file: str, url: str):
        pass

    async def shutdown(self) -> CommandStatus:
        """Stops GFTP service.

        After shutdown all generated urls will be unavailable.
        """
        pass


def service(debug=False) -> AsyncContextManager[GftpDriver]:
    proc = __Process(_debug=debug)
    return cast(AsyncContextManager[GftpDriver], proc)


class __Process(jsonrpc_base.Server):
    def __init__(self, _debug: bool = False):
        super().__init__()
        self._debug = _debug
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._lock = asyncio.Lock()

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

        with contextlib.suppress(Exception):
            await cast(GftpDriver, self).shutdown()

        async with self._lock:
            if p.stdin:
                await p.stdin.drain()
                p.stdin.close()
                try:
                    await asyncio.wait_for(p.wait(), 10.0)
                    return
                except asyncio.TimeoutError:
                    pass
        p.kill()
        ret_code = await p.wait()
        _logger.debug("GFTP server closed, code=%d", ret_code)

    def __log_debug(self, msg_dir: Literal["in", "out"], msg: Union[bytes, str]):
        if self._debug:
            if isinstance(msg, bytes):
                msg = msg.decode(encoding="utf-8")
            stderr = sys.stderr
            stderr.write("\n <= " if msg_dir == "in" else "\n => ")
            stderr.write(msg)
            stderr.flush()

    async def send_message(self, message):
        async with self._lock:
            assert self._proc is not None
            assert self._proc.stdin is not None
            assert self._proc.stdout is not None
            bytes = message.serialize() + "\n"
            self.__log_debug("out", bytes)
            self._proc.stdin.write(bytes.encode("utf-8"))
            await self._proc.stdin.drain()
            msg = await self._proc.stdout.readline()
            self.__log_debug("in", msg)
            if not msg:
                sys.stderr.write("Please check if gftp is installed and is in your $PATH.\n")
                sys.stderr.flush()
            msg = json.loads(msg)
            return message.parse_response(msg)


class GftpSource(Source):
    def __init__(self, length: int, link: PubLink):
        self._len = length
        self._link = link

    @property
    def download_url(self) -> str:
        return self._link["url"]

    @property
    def path(self) -> Path:
        return Path(self._link["file"])

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
                    yield chunk
                    chunk = f.read(30_000)

        return Content(length=length, stream=chunks())

    async def download_file(self, destination_file: PathLike):
        if str(destination_file) == self._link["file"]:
            return
        return await super().download_file(destination_file)


@contextlib.contextmanager
def _temp_file(temp_dir: Path) -> Iterator[Path]:
    """Create a new temporary file in `temp_dir`.

    Implements the ContextManager interface. Deletes the file on ContextManager's exit.
    """
    fd, name = tempfile.mkstemp(prefix="yapapi_", dir=temp_dir)
    os.close(fd)
    path = Path(name)
    try:
        yield path
    finally:
        _delete_if_exists(path)


def _delete_if_exists(path: Path) -> None:
    if path.exists():
        try:
            path.unlink()
            _logger.debug("Deleted temporary file %s", path)
        except PermissionError as err:
            # We're on Windows and using `gftp` < 0.7.3, so the file is kept open
            # by `gftp` and cannot be deleted.
            _logger.debug("Cannot delete file: %s", err)


USE_GFTP_CLOSE_ENV_VAR = "YAPAPI_USE_GFTP_CLOSE"
"""The environment variable used by GftpProvider to control whether `gftp close` should be used."""


def read_use_gftp_close_env_var() -> Optional[bool]:
    """Determine from the environment whether `GftpProvider` should use the `gftp close` command.

    Reads the value of the environment variable with the name stored in `USE_GFTP_CLOSE_ENV_VAR`.
    If the environment variable is set and its value can be interpreted as boolean value by
    `distutils.util.strtobool()` then the corresponding boolean value is returned. Otherwise,
    returns `None`.
    """
    try:
        env_value = os.environ[USE_GFTP_CLOSE_ENV_VAR]
        return distutils.util.strtobool(env_value)
    except Exception:
        return None


MIN_GFTP_VERSION_THAT_CAN_GFTP_CLOSE = semantic_version.Version("0.7.3")


class GftpProvider(StorageProvider, AsyncContextManager[StorageProvider]):
    """A StorageProvider that communicates with `gftp server` through JSON-RPC.

    The provider keeps track of the files published by `gftp` and their URLs.
    If an URL no longer needs to be published then the provider _should_ issue the
    `gftp close URL` command, so the file published with this URL is closed by `gftp`.

    However, `gftp close URL` may cause errors, due to a bug in `gftp` prior to version 0.7.3
    (see https://github.com/golemfactory/yagna/pull/1501). Therefore the provider uses
    the following logic to dermine if it should use the `gftp close URL` command:
    1. If the environment variable `YAPAPI_USE_GFTP_CLOSE` is set to a truthy value,
       then `gftp close URL` will be used.
    2. If the environment variable `YAPAPI_USE_GFTP_CLOSE` is set to a falsy value,
       then `gftp close URL` will not be used.
    3. If neither 1 nor 2 holds and the version reported by `gftp` is 0.7.3 or larger
       (according to Semantic Versioning 2.0.0) then `gftp close URL` will be used.
    4. Otherwise `gftp close URL` will not be used.

    Note: Reading the `YAPAPI_USE_GFTP_CLOSE` variable is done once, when the provider
    is instantiated, and the version check is made in the provider's `__aenter__()` method.
    """

    @dataclass
    class URLInfo:
        """Information about an URL published through `gftp`."""

        publish_count: int
        """Number of `gftp publish` operations for this URL.

        Serves as a reference counter. When it drops to 0, `gftp close {URL}` is invoked
        in order to release any file published with this URL that is kept open by `gftp`.
        Note that the value of this field may be larger than the number of files published,
        since a single file may be published more than once."""

        temporary_files: Set[Path]
        """Set of temporary files published with this URL.

        When the URL is unpublished by calling `gftp close {URL}`, all temporary files with this
        URL can be safely deleted.
        """

    def __init__(self, *, tmpdir: Optional[str] = None):
        self.__exit_stack = AsyncExitStack()

        # Directory for temporal files created by this provider
        self._temp_dir: Optional[Path] = Path(tmpdir) if tmpdir else None

        # Mapping of URLs to info on files published with this URL
        self._published_sources: Dict[str, GftpProvider.URLInfo] = dict()

        # Lock used to synchronize access to self._published_sources
        self._lock: asyncio.Lock = asyncio.Lock()

        # Flag indicating if this `GftpProvider` will close unpublished URLs.
        # See this class' docstring for more details.
        self._close_urls: Optional[bool] = read_use_gftp_close_env_var()

        # Reference to an external process running the `gftp server` command
        self._process: Optional["__Process"] = None

    async def __aenter__(self) -> StorageProvider:
        if not self._temp_dir:
            self._temp_dir = Path(
                self.__exit_stack.enter_context(tempfile.TemporaryDirectory(prefix="yapapi-gftp-"))
            )
            _logger.debug("Creating a temporary directory %s", self._temp_dir)
        process = await self.__get_process()
        gftp_version = await process.version()
        assert gftp_version

        if self._close_urls is None:
            try:
                # Gftp_version could be something like `7.2.3 (10116c7d 2021-07-28 build #164)`,
                # we need to discard everything after the first space.
                semver = semantic_version.Version(gftp_version.split()[0])
                self._close_urls = semver >= MIN_GFTP_VERSION_THAT_CAN_GFTP_CLOSE
                _logger.debug(
                    "Setting _close_urls to %s, gftp version: %s", self._close_urls, gftp_version
                )
            except ValueError:
                _logger.warning("Cannot parse gftp version info '%s'", gftp_version)
                self._close_urls = False
        assert self._close_urls is not None

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.__exit_stack.aclose()
        # Remove temporary files created by this provider
        if not self._temp_dir:
            raise RuntimeError("GftpProvider.__aenter__() not called")
        if self._temp_dir and self._temp_dir.exists():
            for info in self._published_sources.values():
                for path in info.temporary_files:
                    _delete_if_exists(path)

        return None

    def __new_file(self) -> Path:
        if not self._temp_dir:
            raise RuntimeError("GftpProvider.__aenter__() not called")
        return self.__exit_stack.enter_context(_temp_file(self._temp_dir))

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
        return await self.upload_file(file_name, _temporary=True)

    async def upload_file(self, path: os.PathLike, _temporary: bool = False) -> Source:

        path = Path(path)
        _logger.debug("Publishing file %s...", path)
        process = await self.__get_process()

        async with self._lock:

            links = await process.publish(files=[str(path)])
            assert len(links) == 1, "Invalid gftp publish response"

            length = path.stat().st_size

            url = links[0]["url"]

            if url not in self._published_sources:
                info = GftpProvider.URLInfo(
                    publish_count=1,
                    temporary_files=({path} if _temporary else set()),
                )
                self._published_sources[url] = info
            else:
                info = self._published_sources[url]

                if path in info.temporary_files:
                    raise ValueError(f"File {path} already published as temporary")

                if _temporary:
                    info.temporary_files.add(path)
                info.publish_count += 1

            _logger.debug(
                "File %s published with URL = %s, count = %d", path, url, info.publish_count
            )

        source = GftpSource(length, links[0])
        return source

    async def release_source(self, source: Source) -> None:

        if not isinstance(source, GftpSource):
            raise ValueError(f"Expected an instance of GftpSource, got {type(source)} instead")

        url = source.download_url
        _logger.debug("Releasing file %s with URL = %s ...", source.path, url)

        async with self._lock:

            if url not in self._published_sources:
                raise ValueError(
                    f"Trying to release an unpublished URL {url}, path = {source.path}"
                )
            info = self._published_sources[url]
            info.publish_count -= 1

            _logger.debug(
                "File %s released, URL = %s, count = %d", source.path, url, info.publish_count
            )

            if info.publish_count == 0:

                _logger.debug("Unpublishing URL %s...", url)
                if self._close_urls:
                    process = await self.__get_process()
                    await process.close(urls=[url])

                for path in info.temporary_files:
                    _delete_if_exists(path)

                del self._published_sources[url]

    async def new_destination(self, destination_file: Optional[PathLike] = None) -> Destination:
        if destination_file:
            if Path(destination_file).exists():
                destination_file = None
        output_file = str(destination_file) if destination_file else str(self.__new_file())
        process = await self.__get_process()
        link = await process.receive(output_file=output_file)
        return GftpDestination(process, link)


def provider() -> AsyncContextManager[StorageProvider]:
    return GftpProvider()


__all__ = ("service", "provider", "GftpDriver", "PubLink")
