"""Stuff."""

import asyncio
from datetime import timedelta
from typing import Awaitable, Optional, List, Tuple, TYPE_CHECKING

from yapapi.events import CommandExecuted
from yapapi.script.command import *

if TYPE_CHECKING:
    from yapapi.ctx import WorkContext


class Script:
    """Stuff."""

    timeout: Optional[timedelta] = None
    """Time after which this script's execution should be forcefully interrupted."""

    wait_for_results: bool = True
    """Stuff."""

    def __init__(self, context: "WorkContext"):
        self._ctx: "WorkContext" = context
        self._commands: List[Tuple[Command, asyncio.Future]] = []

    def _evaluate(self) -> List[BatchCommand]:
        """Evaluate and serialize this script to a list of batch commands."""
        batch: List[BatchCommand] = []
        for cmd, _future in self._commands:
            batch.append(cmd.evaluate(self._ctx))
        return batch

    async def _after(self):
        """Hook which is executed after the script has been run on the provider."""
        for cmd, _future in self._commands:
            await cmd.after(self._ctx)

    async def _before(self):
        """Hook which is executed before the script is evaluated and sent to the provider."""
        if not self._ctx._started and self._ctx._implicit_init:
            # TODO: maybe check if first two steps already cover this?
            loop = asyncio.get_event_loop()
            self._commands.insert(0, (Deploy(), loop.create_future()))
            self._commands.insert(1, (Start(), loop.create_future()))

        for cmd, _future in self._commands:
            await cmd.before(self._ctx)

    def _set_cmd_result(self, result: CommandExecuted) -> None:
        self._commands[result.cmd_idx][1].set_result(result)

    def add(self, cmd: Command) -> Awaitable[CommandExecuted]:
        loop = asyncio.get_event_loop()
        future_result = loop.create_future()
        self._commands.append((cmd, future_result))
        return future_result

    def deploy(self) -> Awaitable[CommandExecuted]:
        """Schedule a Deploy command on the provider."""
        return self.add(Deploy())

    def start(self, *args: str) -> Awaitable[CommandExecuted]:
        """Schedule a Start command on the provider."""
        return self.add(Start(*args))

    def terminate(self) -> Awaitable[CommandExecuted]:
        """Schedule a Terminate command on the provider."""
        return self.add(Terminate())

    def send_json(self, data: dict, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending JSON data to the provider.

        :param data: dictionary representing JSON data to send
        :param dst_path: remote (provider) destination path
        """
        return self.add(SendJson(data, dst_path))

    def send_bytes(self, data: bytes, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending bytes data to the provider.

        :param data: bytes to send
        :param dst_path: remote (provider) destination path
        """
        return self.add(SendBytes(data, dst_path)

    def send_file(self, src_path: str, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending a file to the provider.

        :param src_path: local (requestor) source path
        :param dst_path: remote (provider) destination path
        """
        return self.add(SendFile(src_path, dst_path)

    def run(
        self,
        cmd: str,
        *args: Iterable[str],
        env: Optional[Dict[str, str]] = None,
        stderr: CaptureContext = CaptureContext.build(mode="stream"),
        stdout: CaptureContext = CaptureContext.build(mode="stream"),
    ) -> Awaitable[CommandExecuted]:
        """Schedule running a shell command on the provider.

        :param cmd: command to run on the provider, e.g. /my/dir/run.sh
        :param args: command arguments, e.g. "input1.txt", "output1.txt"
        :param env: optional dictionary with environment variables
        :param stderr: capture context to use for stderr
        :param stdout: capture context to use for stdout
        """
        return self.add(Run(cmd, *args, env=env, stdout=stdout, stderr=stderr))

    def download_file(self, src_path: str, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule downloading a remote file from the provider.

        :param src_path: remote (provider) source path
        :param dst_path: local (requestor) destination path
        """
        return self.add(DownloadFile(src_path, dst_path))

    def download_bytes(
        self,
        src_path: str,
        on_download: Callable[[bytes], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
   ) -> Awaitable[CommandExecuted]:
        """Schedule downloading a remote file from the provider as bytes.

        :param src_path: remote (provider) source path
        :param on_download: the callable to run on the received data
        :param limit: limit of bytes to be downloaded (expected size)
        """
        return self.add(DownloadBytes(src_path, on_download, limit))

    def download_json(
        self,
        src_path: str,
        on_download: Callable[[Any], Awaitable],
        limit: int = DOWNLOAD_BYTES_LIMIT_DEFAULT,
   ) -> Awaitable[CommandExecuted]:
        """Schedule downloading a remote file from the provider as JSON.

        :param src_path: remote (provider) source path
        :param on_download: the callable to run on the received data
        :param limit: limit of bytes to be downloaded (expected size)
        """
        return self.add(DownloadJson(src_path, on_download, limit))
