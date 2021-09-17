from datetime import timedelta
import itertools
from typing import Any, Awaitable, Callable, Dict, Iterator, Optional, List, TYPE_CHECKING

import yapapi
from yapapi.events import CommandExecuted
from yapapi.script.capture import CaptureContext
from yapapi.script.command import (
    BatchCommand,
    Command,
    Deploy,
    DownloadBytes,
    DownloadFile,
    DownloadJson,
    Run,
    SendBytes,
    SendFile,
    SendJson,
    Start,
    Terminate,
)
from yapapi.storage import DOWNLOAD_BYTES_LIMIT_DEFAULT

if TYPE_CHECKING:
    from yapapi.ctx import WorkContext

script_ids: Iterator[int] = itertools.count(1)
"""An iterator providing incremental integer IDs to scripts."""


class Script:
    """Represents a series of commands to be executed on a provider node.

    New commands are added to the script either through its :func:`add` method or by calling one of the
    convenience methods provided (for example: :func:`run` or :func:`upload_json`).
    Adding a new command *does not* result in it being immediately executed. Once ready, a :class:`Script`
    instance is meant to be yielded from a worker function (work generator pattern).
    Commands will be run in the order in which they were added to the script.
    """

    def __init__(
        self,
        context: "yapapi.ctx.WorkContext",
        timeout: Optional[timedelta] = None,
        wait_for_results: bool = True,
    ):
        """Initialize a :class:`Script`

        :param context: A :class:`yapapi.WorkContext` that will be used to evaluate the script (i.e. to send
            commands to the provider)
        :param timeout: Time after which this script's execution should be forcefully interrupted.
            The default value is `None` which means there's no timeout set.
        :param wait_for_results: Whether this script's execution should block until its results are available.
            The default value is `True`.
        """
        self.timeout = timeout
        self.wait_for_results = wait_for_results
        self._ctx: "WorkContext" = context
        self._commands: List[Command] = []
        self._id: int = next(script_ids)

    @property
    def id(self) -> int:
        """Return the ID of this :class:`Script` instance.

        IDs are provided by a global iterator and therefore are guaranteed to be unique during
        the program's execution.
        """
        return self._id

    def _evaluate(self) -> List[BatchCommand]:
        """Evaluate and serialize this script to a list of batch commands."""
        batch: List[BatchCommand] = []
        for cmd in self._commands:
            batch.append(cmd.evaluate(self._ctx))
        return batch

    async def _after(self):
        """Hook which is executed after the script has been run on the provider."""
        for cmd in self._commands:
            await cmd.after(self._ctx)

    async def _before(self):
        """Hook which is executed before the script is evaluated and sent to the provider."""
        for cmd in self._commands:
            await cmd.before(self._ctx)

    def _set_cmd_result(self, cmd_event: CommandExecuted) -> None:
        cmd = self._commands[cmd_event.cmd_idx]
        cmd._result.set_result(cmd_event)

    def add(self, cmd: Command) -> Awaitable[CommandExecuted]:
        """Add a :class:`yapapi.script.command.Command` to the :class:`Script`"""
        self._commands.append(cmd)
        return cmd._result

    def deploy(self, **kwargs: dict) -> Awaitable[CommandExecuted]:
        """Schedule a :class:`Deploy` command on the provider."""
        return self.add(Deploy(**kwargs))

    def start(self, *args: str) -> Awaitable[CommandExecuted]:
        """Schedule a :class:`Start` command on the provider."""
        return self.add(Start(*args))

    def terminate(self) -> Awaitable[CommandExecuted]:
        """Schedule a :class:`Terminate` command on the provider."""
        return self.add(Terminate())

    def run(
        self,
        cmd: str,
        *args: str,
        env: Optional[Dict[str, str]] = None,
        stderr: Optional[CaptureContext] = None,
        stdout: Optional[CaptureContext] = None,
    ) -> Awaitable[CommandExecuted]:
        """Schedule running a shell command on the provider.

        :param cmd: command to run on the provider, e.g. /my/dir/run.sh
        :param args: command arguments, e.g. "input1.txt", "output1.txt"
        :param env: optional dictionary with environment variables
        :param stderr: capture context to use for stderr
        :param stdout: capture context to use for stdout
        """
        kwargs: dict = {"env": env}
        if stdout is not None:
            kwargs["stdout"] = stdout
        if stderr is not None:
            kwargs["stderr"] = stderr

        return self.add(Run(cmd, *args, **kwargs))

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

    def download_file(self, src_path: str, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule downloading a remote file from the provider.

        :param src_path: remote (provider) source path
        :param dst_path: local (requestor) destination path
        """
        return self.add(DownloadFile(src_path, dst_path))

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

    def upload_bytes(self, data: bytes, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending bytes data to the provider.

        :param data: bytes to send
        :param dst_path: remote (provider) destination path
        """
        return self.add(SendBytes(data, dst_path))

    def upload_file(self, src_path: str, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending a file to the provider.

        :param src_path: local (requestor) source path
        :param dst_path: remote (provider) destination path
        """
        return self.add(SendFile(src_path, dst_path))

    def upload_json(self, data: dict, dst_path: str) -> Awaitable[CommandExecuted]:
        """Schedule sending JSON data to the provider.

        :param data: dictionary representing JSON data to send
        :param dst_path: remote (provider) destination path
        """
        return self.add(SendJson(data, dst_path))
