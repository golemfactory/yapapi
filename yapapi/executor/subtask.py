import asyncio
from datetime import datetime, timedelta
from enum import Enum, auto
import itertools
from typing import Callable, ClassVar, Iterator, Generic, Optional, Set, Tuple, TypeVar, Union

from . import events
from ._smartq import SmartQueue, Handle


class SubTaskStatus(Enum):
    WAITING = auto()
    RUNNING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


Data = TypeVar("Data")
Result = TypeVar("Result")
TaskEvents = Union[events.SubTaskAccepted, events.SubTaskRejected]


class SubTask(Generic[Data, Result]):
    """One computation unit.

    Represents one computation unit that will be run on the provider
    (e.g. rendering of one frame of an animation).
    """

    ids: ClassVar[Iterator[int]] = itertools.count(1)

    def __init__(
        self,
        data: Data,
        *,
        expires: Optional[datetime] = None,
        timeout: Optional[timedelta] = None,
    ):
        """Create a new SubTask object.

        :param data: contains information needed to prepare command list for the provider
        :param expires: expiration datetime
        :param timeout: timeout from now; overrides expires parameter if provided
        """
        self.id: str = str(next(SubTask.ids))
        self._started = datetime.now()
        self._expires: Optional[datetime]
        self._emit: Optional[Callable[[TaskEvents], None]] = None
        self._callbacks: Set[Callable[["SubTask[Data, Result]", SubTaskStatus], None]] = set()
        self._handle: Optional[
            Tuple[Handle["SubTask[Data, Result]"], SmartQueue["SubTask[Data, Result]"]]
        ] = None
        if timeout:
            self._expires = self._started + timeout
        else:
            self._expires = expires

        self._result: Optional[Result] = None
        self._data = data
        self._status: SubTaskStatus = SubTaskStatus.WAITING

    def _add_callback(
        self, callback: Callable[["SubTask[Data, Result]", SubTaskStatus], None]
    ) -> None:
        self._callbacks.add(callback)

    def __repr__(self) -> str:
        return f"SubTask(id={self.id}, data={self._data})"

    def _start(self, emitter: Callable[[TaskEvents], None]) -> None:
        self._status = SubTaskStatus.RUNNING
        self._emit = emitter

    def _stop(self, retry: bool = False):
        if self._handle:
            (handle, queue) = self._handle
            loop = asyncio.get_event_loop()
            if retry:
                loop.create_task(queue.reschedule(handle))
            else:
                loop.create_task(queue.mark_done(handle))

    @staticmethod
    def for_handle(
        handle: Handle["SubTask[Data, Result]"],
        queue: SmartQueue["SubTask[Data, Result]"],
        emitter: Callable[[events.Event], None],
    ) -> "SubTask[Data, Result]":
        subtask = handle.data
        subtask._handle = (handle, queue)
        subtask._start(emitter)
        return subtask

    @property
    def data(self) -> Data:
        return self._data

    @property
    def output(self) -> Optional[Result]:
        return self._result

    @property
    def expires(self) -> Optional[datetime]:
        return self._expires

    def accept_result(self, result: Optional[Result] = None) -> None:
        """Accept subtask that was completed.

        Must be called when the results of the subtask are correct.

        :param result: computation result (optional)
        :return: None
        """
        if self._emit:
            self._emit(events.SubTaskAccepted(subtask_id=self.id, result=result))
        assert self._status == SubTaskStatus.RUNNING, "Accepted subtask not running"
        self._status = SubTaskStatus.ACCEPTED
        self._result = result
        self._stop()
        for cb in self._callbacks:
            cb(self, SubTaskStatus.ACCEPTED)

    def reject_result(self, reason: Optional[str] = None, retry: bool = False) -> None:
        """Reject subtask.

        Must be called when the results of the subtask
        are not correct and it should be retried.

        :param reason: subtask rejection description (optional)
        :return: None
        """
        if self._emit:
            self._emit(events.SubTaskRejected(subtask_id=self.id, reason=reason))
        assert self._status == SubTaskStatus.RUNNING, "Rejected task not running"
        self._status = SubTaskStatus.REJECTED
        self._stop(retry)

        for cb in self._callbacks:
            cb(self, SubTaskStatus.REJECTED)
