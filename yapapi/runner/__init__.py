from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional
import abc
from dataclasses import dataclass


class Engine(object):
    pass


class TaskStatus(Enum):
    WAITING = auto()
    RUNNING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


class Task:
    def __init__(
        self,
        *,
        expires: Optional[datetime] = None,
        timeout: Optional[timedelta] = None,
        **kwargs
    ):
        self._started = datetime.now()
        self._expires: Optional[datetime]
        if timeout:
            self._expires = self._started + timeout
        else:
            self._expires = expires

        self._data = kwargs
        self._status: TaskStatus = TaskStatus.WAITING

    def __getattr__(self, item):
        return self._data.get(item)

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
