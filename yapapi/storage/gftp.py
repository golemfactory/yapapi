"""
Golem File Transfer Storage Provider
"""

import asyncio
import json
import sys
from typing import List, Optional, cast, Union
import contextlib
import jsonrpc_base
from typing_extensions import Protocol, Literal, TypedDict, AsyncContextManager


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
        self._proc = await asyncio.create_subprocess_shell(
            "gftp server", stdout=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.PIPE
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
