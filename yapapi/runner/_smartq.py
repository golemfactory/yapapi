""" YAPAPI internal module. This is not a part of the public API. It can change at any time.


"""

from asyncio.locks import Lock, Condition
from types import TracebackType
from typing import (
    Iterable,
    TypeVar,
    Generic,
    AsyncIterator,
    Set,
    Optional,
    Iterator,
    ContextManager,
    Type,
    Dict,
)
from typing_extensions import AsyncIterable
import asyncio
import logging

_logger = logging.getLogger("yapapi.runner")
Item = TypeVar("Item")


class Handle(Generic[Item], object):
    __slots__ = ("_data", "_prev_consumers", "_consumer")

    def __init__(self, data: Item, *, consumer: Optional["Consumer[Item]"] = None):
        self._data = data
        self._prev_consumers: Set["Consumer[Item]"] = set()
        if consumer is not None:
            self._prev_consumers.add(consumer)

        self._consumer = consumer

    @property
    def consumer(self):
        return self._consumer

    def assign_consumer(self, consumer: "Consumer[Item]") -> None:
        self._prev_consumers.add(consumer)
        self._consumer = consumer

    @property
    def data(self) -> Item:
        return self._data


class SmartQueue(Generic[Item], object):
    def __init__(self, items: Iterable[Item], *, retry_cnt: int = 2):
        self._items: Optional[Iterator[Item]] = iter(items)
        self._rescheduled_items: Set[Handle[Item]] = set()
        self._in_progress: Set[Handle[Item]] = set()

        # Synchronization primitives
        self._lock = Lock()
        self._new_items = Condition(lock=self._lock)
        self._eof = Condition(lock=self._lock)

    def new_consumer(self) -> "Consumer[Item]":
        return Consumer(self)

    def __have_data(self):
        return self._items is not None or bool(self._rescheduled_items) or bool(self._in_progress)

    def __find_rescheduled_item(self, consumer: "Consumer[Item]") -> Optional[Handle[Item]]:
        return next(
            (
                handle
                for handle in self._rescheduled_items
                if consumer not in handle._prev_consumers
            ),
            None,
        )

    async def get(self, consumer: "Consumer[Item]") -> Handle[Item]:
        async with self._lock:
            while self.__have_data():
                handle = self.__find_rescheduled_item(consumer)
                if handle:
                    self._rescheduled_items.remove(handle)
                    self._in_progress.add(handle)
                    handle.assign_consumer(consumer)
                    return handle

                if self._items:
                    next_elem = next(self._items, None)
                    if next_elem is None:
                        self._items = None
                        if not self._rescheduled_items and not self._in_progress:
                            self._new_items.notify_all()
                            raise StopAsyncIteration
                    else:
                        handle = Handle(next_elem, consumer=consumer)
                        self._in_progress.add(handle)
                        return handle
                await self._new_items.wait()
        raise StopAsyncIteration

    async def mark_done(self, handle: Handle[Item]) -> None:
        assert handle in self._in_progress, "handle is not in progress"
        async with self._lock:
            self._in_progress.remove(handle)
            self._eof.notify_all()
            self._new_items.notify_all()
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"status in-progress={len(self._in_progress)}, have_item={bool(self._items)}"
            )

    async def reschedule(self, handle: Handle[Item]) -> None:
        assert handle in self._in_progress, "handle is not in progress"
        async with self._lock:
            self._in_progress.remove(handle)
            self._rescheduled_items.add(handle)
            self._new_items.notify_all()

    async def reschedule_all(self, consumer: "Consumer[Item]"):
        async with self._lock:
            handles = [handle for handle in self._in_progress if handle.consumer == consumer]
            for handle in handles:
                self._in_progress.remove(handle)
                self._rescheduled_items.add(handle)
            self._new_items.notify_all()

    def stats(self) -> Dict:
        return {
            "locked": self._lock.locked(),
            "items": bool(self._items),
            "in-progress": len(self._in_progress),
            "rescheduled-items": len(self._rescheduled_items),
        }

    async def wait_until_done(self) -> None:
        async with self._lock:
            while self.__have_data():
                await self._eof.wait()


class Consumer(
    Generic[Item],
    AsyncIterator[Handle[Item]],
    AsyncIterable[Handle[Item]],
    ContextManager["Consumer[Item]"],
):
    def __init__(self, queue: SmartQueue[Item]):
        self._queue = queue
        self._fetched: Optional[Handle[Item]] = None

    def __enter__(self) -> "Consumer[Item]":
        return self

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        asyncio.get_event_loop().create_task(self._queue.reschedule_all(self))
        return None

    @property
    def last_item(self) -> Optional[Item]:
        return self._fetched.data if self._fetched else None

    async def __anext__(self) -> Handle[Item]:
        val = await self._queue.get(self)
        self._fetched = val
        return val
