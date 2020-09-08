import abc
import json
from pathlib import Path
from typing import Iterable, Optional, Dict, List, Tuple, TYPE_CHECKING

from .events import EventEmitter, StorageEvent
from ..storage import StorageProvider, Source, Destination

if TYPE_CHECKING:
    from . import Task


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
        # Executes before commands are send to provider.
        pass

    def register(self, commands: CommandContainer):
        pass

    async def post(self):
        pass


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
    def __init__(self, cmd: str, *args: Iterable[str], env: Optional[Dict[str, str]] = None):
        self.cmd = cmd
        self.args = args
        self.env = env
        self._idx = None

    def register(self, commands: CommandContainer):
        self._idx = commands.run(entry_point=self.cmd, args=self.args)


class _RecvFile(Work):
    def __init__(
        self,
        storage: StorageProvider,
        src_path: str,
        dst_path: str,
        emitter: Optional[EventEmitter[StorageEvent]] = None,
    ):
        self._storage = storage
        self._dst_path = Path(dst_path)
        self._src_path: str = src_path
        self._idx: Optional[int] = None
        self._dst_slot: Optional[Destination] = None
        self._emitter: Optional[EventEmitter[StorageEvent]] = emitter

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
            self._emitter(StorageEvent.DOWNLOAD_STARTED, self._src_path)
        await self._dst_slot.download_file(self._dst_path)
        if self._emitter:
            self._emitter(StorageEvent.DOWNLOAD_FINISHED, self._src_path)


class _Steps(Work):
    def __init__(self, *steps: Work):
        self._steps: Tuple[Work, ...] = steps

    async def prepare(self):
        for step in self._steps:
            await step.prepare()

    def register(self, commands: CommandContainer):
        for step in self._steps:
            step.register(commands)

    async def post(self):
        for step in self._steps:
            await step.post()


class WorkContext:
    """Should be used to define commands to be sent to the provider
    """

    def __init__(
        self,
        ctx_id: str,
        storage: StorageProvider,
        emitter: Optional[EventEmitter[StorageEvent]] = None,
    ):
        self._id = ctx_id
        self._storage: StorageProvider = storage
        self._pending_steps: List[Work] = []
        self._started: bool = False
        self._emitter: Optional[EventEmitter[StorageEvent]] = emitter

    def __prepare(self):
        if not self._started:
            self._pending_steps.append(_InitStep())
            self._started = True

    def begin(self):
        """Begins command list definition. Currently does nothing.
        :return: None
        """
        pass

    def send_json(self, json_path: str, data: dict):
        """Schedules sending JSON data to the provider
        :param json_path: remote (provider) path
        :param data: dictionary representing JSON data
        :return: None
        """
        self._pending_steps.append(_SendJson(self._storage, data, json_path))

    def send_file(self, src_path: str, dst_path: str):
        """Schedules sending file to the provider
        :param src_path: local (requestor) path
        :param dst_path: remote (provider) path
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_SendFile(self._storage, src_path, dst_path))

    def run(self, cmd: str, *args: Iterable[str], env: Optional[Dict[str, str]] = None):
        """Schedules running a command
        :param cmd: command to run on the provider, e.g. /my/dir/run.sh
        :param args: command arguments, e.g. "input1.txt", "output1.txt"
        :param env: optional dictionary with environmental variables
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_Run(cmd, *args, env=env))

    def download_file(self, src_path: str, dst_path: str):
        """Schedules downloading remote file from the provider
        :param src_path: remote (provider) path
        :param dst_path: local (requestor) path
        :return: None
        """
        self.__prepare()
        self._pending_steps.append(_RecvFile(self._storage, src_path, dst_path, self._emitter))

    def log(self, *args):
        print(f"W{self._id}: ", *args)

    def commit(self, task: "Task") -> Tuple["Task", Work]:
        """Ends task-related command list definition
        :param task: task related to the list of commands
        :return: this return value should be yielded from the worker() function
        """
        steps = self._pending_steps
        self._pending_steps = []
        return task, _Steps(*steps)
