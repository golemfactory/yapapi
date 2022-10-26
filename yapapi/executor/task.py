import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
import itertools
from typing import (
    Callable,
    ClassVar,
    Generic,
    Iterator,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

from yapapi import events

from ._smartq import Handle, SmartQueue


class TaskStatus(Enum):
    WAITING = auto()
    RUNNING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


TaskData = TypeVar("TaskData")
TaskResult = TypeVar("TaskResult")
TaskEvents = Union[events.TaskAccepted, events.TaskRejected]


class TaskEmitter(Protocol):
    def __call__(self, event_class: Type[events.TaskEventType], **kwargs) -> events.TaskEventType:
        ...


class Task(Generic[TaskData, TaskResult]):
    """One computation unit.

    Represents one computation unit that will be run on the provider
    (e.g. rendering of one frame of an animation).
    """

    ids: ClassVar[Iterator[int]] = itertools.count(1)

    def __init__(
        self,
        data: TaskData,
    ):
        """Create a new :class:`Task` object.

        :param data: contains information needed to prepare command list for the provider
        """
        self.id: str = str(next(Task.ids))
        self._started: Optional[datetime] = None
        self._finished: Optional[datetime] = None
        self._emit: Optional[TaskEmitter] = None
        self._callbacks: Set[Callable[["Task[TaskData, TaskResult]", TaskStatus], None]] = set()
        self._handle: Optional[
            Tuple[Handle["Task[TaskData, TaskResult]"], SmartQueue["Task[TaskData, TaskResult]"]]
        ] = None

        self._result: Optional[TaskResult] = None
        self._data = data
        self._status: TaskStatus = TaskStatus.WAITING

    def emit(self, event_class: Type[events.TaskEventType], **kwargs) -> events.TaskEventType:
        if self._emit is None:
            raise RuntimeError("Task {self} haven't started yet, so it can't emit")
        return self._emit(event_class, task=self, **kwargs)

    def _add_callback(
        self, callback: Callable[["Task[TaskData, TaskResult]", TaskStatus], None]
    ) -> None:
        self._callbacks.add(callback)

    def __repr__(self) -> str:
        return f"Task(id={self.id}, data={self._data})"

    def _start(self, emitter: TaskEmitter) -> None:
        self._status = TaskStatus.RUNNING
        self._emit = emitter
        self._started = datetime.now(timezone.utc)
        self._finished = None

    def _stop(self, retry: bool = False):
        self._finished = datetime.now(timezone.utc)
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
        emitter: TaskEmitter,
    ) -> "Task[TaskData, TaskResult]":
        task = handle.data
        task._handle = (handle, queue)
        task._start(emitter)
        return task

    @property
    def data(self) -> TaskData:
        return self._data

    @property
    def result(self) -> Optional[TaskResult]:
        return self._result

    @property
    def running_time(self) -> Optional[timedelta]:
        """Return the running time of the task (if in progress) or time it took to complete it."""
        if self._finished:
            assert self._started
            return self._finished - self._started
        if self._started:
            return datetime.now(timezone.utc) - self._started
        return None

    def accept_result(self, result: Optional[TaskResult] = None) -> None:
        """Accept the result of this task.

        Must be called when the result is correct to mark this task
        as completed.

        :param result: task computation result (optional)
        """
        assert self._status == TaskStatus.RUNNING, "Accepted task not running"
        self._status = TaskStatus.ACCEPTED
        self._result = result
        self.emit(events.TaskAccepted)
        self._stop()
        for cb in self._callbacks:
            cb(self, TaskStatus.ACCEPTED)

    def reject_result(self, reason: Optional[str] = None, retry: bool = False) -> None:
        """Reject the result of this task.

        Must be called when the result is not correct to indicate
        that the task should be retried.

        :param reason: task rejection description (optional)
        """
        self.emit(events.TaskRejected, reason=reason)
        assert self._status == TaskStatus.RUNNING, "Rejected task not running"
        self._status = TaskStatus.REJECTED
        self._stop(retry)

        for cb in self._callbacks:
            cb(self, TaskStatus.REJECTED)
