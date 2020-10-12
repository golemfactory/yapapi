import asyncio
from datetime import datetime, timedelta
from enum import Enum, auto
import itertools
from typing import Callable, ClassVar, Iterator, Generic, Optional, Set, Tuple, TypeVar, Union

from . import events
from ._smartq import SmartQueue, Handle


class TaskStatus(Enum):
    WAITING = auto()
    RUNNING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


TaskData = TypeVar("TaskData")
TaskResult = TypeVar("TaskResult")
TaskEvents = Union[events.TaskAccepted, events.TaskRejected]


class Task(Generic[TaskData, TaskResult]):
    """One computation unit.

    Represents one computation unit that will be run on the provider
    (e.g. rendering of one frame of an animation).
    """

    ids: ClassVar[Iterator[int]] = itertools.count(1)

    def __init__(
        self,
        data: TaskData,
        *,
        expires: Optional[datetime] = None,
        timeout: Optional[timedelta] = None,
    ):
        """Create a new Task object.

        :param data: contains information needed to prepare command list for the provider
        :param expires: expiration datetime
        :param timeout: timeout from now; overrides expires parameter if provided
        """
        self.id: str = str(next(Task.ids))
        self._started = datetime.now()
        self._expires: Optional[datetime]
        self._emit: Optional[Callable[[TaskEvents], None]] = None
        self._callbacks: Set[Callable[["Task[TaskData, TaskResult]", TaskStatus], None]] = set()
        self._handle: Optional[
            Tuple[Handle["Task[TaskData, TaskResult]"], SmartQueue["Task[TaskData, TaskResult]"]]
        ] = None
        if timeout:
            self._expires = self._started + timeout
        else:
            self._expires = expires

        self._result: Optional[TaskResult] = None
        self._data = data
        self._status: TaskStatus = TaskStatus.WAITING

    def _add_callback(
        self, callback: Callable[["Task[TaskData, TaskResult]", TaskStatus], None]
    ) -> None:
        self._callbacks.add(callback)

    def __repr__(self) -> str:
        return f"Task(id={self.id}, data={self._data})"

    def _start(self, emitter: Callable[[TaskEvents], None]) -> None:
        self._status = TaskStatus.RUNNING
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
        handle: Handle["Task[TaskData, TaskResult]"],
        queue: SmartQueue["Task[TaskData, TaskResult]"],
        emitter: Callable[[events.Event], None],
    ) -> "Task[TaskData, TaskResult]":
        task = handle.data
        task._handle = (handle, queue)
        task._start(emitter)
        return task

    @property
    def data(self) -> TaskData:
        return self._data

    @property
    def output(self) -> Optional[TaskResult]:
        return self._result

    @property
    def expires(self) -> Optional[datetime]:
        return self._expires

    def accept_task(self, result: Optional[TaskResult] = None) -> None:
        """Accept task that was completed.

        Must be called when the results of a task are correct.

        :param result: computation result (optional)
        :return: None
        """
        if self._emit:
            self._emit(events.TaskAccepted(task_id=self.id, result=result))
        assert self._status == TaskStatus.RUNNING, "Accepted task not running"
        self._status = TaskStatus.ACCEPTED
        self._result = result
        self._stop()
        for cb in self._callbacks:
            cb(self, TaskStatus.ACCEPTED)

    def reject_task(self, reason: Optional[str] = None, retry: bool = False) -> None:
        """Reject task.

        Must be called when the results of the task
        are not correct and it should be retried.

        :param reason: task rejection description (optional)
        :return: None
        """
        if self._emit:
            self._emit(events.TaskRejected(task_id=self.id, reason=reason))
        assert self._status == TaskStatus.RUNNING, "Rejected task not running"
        self._status = TaskStatus.REJECTED
        self._stop(retry)

        for cb in self._callbacks:
            cb(self, TaskStatus.REJECTED)
