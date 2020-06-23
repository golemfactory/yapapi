"""

"""
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional, TypeVar, Generic, AsyncContextManager
import abc


class Engine(AsyncContextManager):
    async def __aenter__(self):
        return self


class TaskStatus(Enum):
    WAITING = auto()
    RUNNING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


TaskData = TypeVar("TaskData")


class Task(Generic[TaskData]):
    def __init__(
        self,
        data: TaskData,
        *,
        expires: Optional[datetime] = None,
        timeout: Optional[timedelta] = None,
    ):
        self._started = datetime.now()
        self._expires: Optional[datetime]
        if timeout:
            self._expires = self._started + timeout
        else:
            self._expires = expires

        self._data = data
        self._status: TaskStatus = TaskStatus.WAITING

    @property
    def data(self) -> TaskData:
        return self._data

    @property
    def expires(self):
        return self._expires

    def accept_task(self):
        assert self._status == TaskStatus.RUNNING
        self._status = TaskStatus.ACCEPTED

    def reject_task(self):
        assert self._status == TaskStatus.RUNNING
        self._status = TaskStatus.REJECTED


class Package(abc.ABC):
    pass
