"""
Storage models.
"""

import abc
import aiohttp
import asyncio
import io
import os
from os import PathLike
import pathlib
from typing import AsyncIterator, NamedTuple, Optional, Union

_BUF_SIZE = 40960
DOWNLOAD_BYTES_LIMIT_DEFAULT = 1 * 1024 * 1024
AsyncReader = Union[asyncio.streams.StreamReader, aiohttp.streams.StreamReader]


class Content(NamedTuple):
    length: int
    stream: AsyncIterator[bytes]

    @classmethod
    def from_reader(cls, length: int, s: AsyncReader):
        async def stream() -> AsyncIterator[bytes]:
            while not s.at_eof():
                buf = await s.read(_BUF_SIZE)
                yield buf

        return Content(length, stream())


class Source(abc.ABC):
    @property
    @abc.abstractmethod
    def download_url(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def content_length(self) -> int:
        raise NotImplementedError


class Destination(abc.ABC):
    @property
    @abc.abstractmethod
    def upload_url(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def download_stream(self) -> Content:
        raise NotImplementedError

    async def download_bytes(self, limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT):
        output = b""
        content = await self.download_stream()

        async for chunk in content.stream:
            limit_remaining = limit - len(output)
            if limit_remaining > len(chunk):
                output += chunk
            else:
                output += chunk[:limit_remaining]
                break

        return output

    async def download_file(self, destination_file: PathLike):
        content = await self.download_stream()
        with open(destination_file, "wb") as f:
            async for chunk in content.stream:
                f.write(chunk)


class InputStorageProvider(abc.ABC):
    @abc.abstractmethod
    async def upload_stream(self, length: int, stream: AsyncIterator[bytes]) -> Source:
        raise NotImplementedError

    async def upload_bytes(self, data: bytes) -> Source:
        async def _inner():
            yield data

        return await self.upload_stream(len(data), _inner())

    async def upload_file(self, path: os.PathLike) -> Source:
        fp = pathlib.Path(path)
        file_size = fp.stat().st_size

        async def read_file():
            with io.open(path, "rb") as f:
                while True:
                    b: bytes = f.read(_BUF_SIZE)
                    if not b:
                        break
                    yield b

        return await self.upload_stream(file_size, read_file())

    async def release_source(self, source: Source) -> None:
        """Release a source returned by `upload_file` or `upload_bytes`.

        The default implementation is to do nothing."""
        pass


class OutputStorageProvider(abc.ABC):
    @abc.abstractmethod
    async def new_destination(self, destination_file: Optional[PathLike] = None) -> Destination:
        """
        Creates slot for receiving file.

        Parameters
        ----------
        destination_file:
            Optional hint where received data should be placed.

        """
        raise NotImplementedError


class StorageProvider(InputStorageProvider, OutputStorageProvider, abc.ABC):
    pass


class ComposedStorageProvider(StorageProvider):
    def __init__(self, input_storage: InputStorageProvider, output_storage: OutputStorageProvider):
        self._input = input_storage
        self._output = output_storage

    async def upload_stream(self, length: int, stream: AsyncIterator[bytes]) -> Source:
        return await self._input.upload_stream(length, stream)

    async def upload_file(self, path: os.PathLike) -> Source:
        return await self._input.upload_file(path)

    async def new_destination(self, destination_file: Optional[PathLike] = None) -> Destination:
        return await self._output.new_destination(destination_file)
