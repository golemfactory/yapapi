""" YAPAPI internal module. This is not a part of the public API. It can change at any time.


"""

from asyncio.locks import Lock, Condition
from types import TracebackType
from typing import (
    TypeVar,
    Generic,
    AsyncIterator,
    Set,
    Optional,
    ContextManager,
    Type,
    Dict,
)
from typing_extensions import AsyncIterable
import asyncio
import logging


_logger = logging.getLogger("yapapi.executor")
Item = TypeVar("Item")


class Handle(Generic[Item]):
    """
    Handle of the queue item, iow, binding between a queue item and a specific consumer.

    Additionally it keeps track of the previously used consumers of the given item
    to prevent them from being assigned to this item again.
    """

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


class SmartQueue(Generic[Item]):
    def __init__(self, items: AsyncIterator[Item]):
        """
        :param items: the items to be iterated over
        """

        self._buffer: "asyncio.Queue[Item]" = asyncio.Queue(maxsize=1)
        self._incoming_finished = False
        self._buffer_task = asyncio.get_event_loop().create_task(self._fill_buffer(items))

        """The items scheduled for reassignment to another consumer"""
        self._rescheduled_items: Set[Handle[Item]] = set()

        """The items currently assigned to consumers"""
        self._in_progress: Set[Handle[Item]] = set()

        # Synchronization primitives
        self._lock = Lock()
        self._new_items = Condition(lock=self._lock)
        self._eof = Condition(lock=self._lock)

    async def _fill_buffer(self, incoming: AsyncIterator[Item]):
        try:
            async for item in incoming:
                await self._buffer.put(item)
                async with self._lock:
                    self._new_items.notify_all()
            self._incoming_finished = True
            async with self._lock:
                self._eof.notify_all()
                self._new_items.notify_all()
        except asyncio.CancelledError:
            pass

    async def close(self):
        if self._buffer_task:
            self._buffer_task.cancel()
            await self._buffer_task
            self._buffer_task = None

    def finished(self):
        return (
            not self.has_unassigned_items() and not (self._in_progress) and self._incoming_finished
        )

    def has_unassigned_items(self) -> bool:
        """Check if this queue has a new or rescheduled item immediately available."""
        return bool(self._rescheduled_items) or bool(self._buffer.qsize())

    def new_consumer(self) -> "Consumer[Item]":
        return Consumer(self)

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
        """Get a handle to the next item to be processed (either a new one or rescheduled)."""
        async with self._lock:
            while not self.finished():

                handle = self.__find_rescheduled_item(consumer)
                if handle:
                    self._rescheduled_items.remove(handle)
                    self._in_progress.add(handle)
                    handle.assign_consumer(consumer)
                    return handle

                if self._buffer.qsize():
                    next_elem = await self._buffer.get()
                    handle = Handle(next_elem, consumer=consumer)
                    self._in_progress.add(handle)
                    return handle

                await self._new_items.wait()
            self._new_items.notify_all()
        raise StopAsyncIteration

    async def mark_done(self, handle: Handle[Item]) -> None:
        """Mark an item, referred to by `handle`, as done."""
        assert handle in self._in_progress, "handle is not in progress"
        async with self._lock:
            self._in_progress.remove(handle)
            self._eof.notify_all()
            self._new_items.notify_all()
        if _logger.isEnabledFor(logging.DEBUG):
            stats = self.stats()
            _logger.debug("status: " + ", ".join(f"{key}: {val}" for key, val in stats.items()))

    async def reschedule(self, handle: Handle[Item]) -> None:
        """Free the item for reassignment to another consumer."""
        assert handle in self._in_progress, "handle is not in progress"
        async with self._lock:
            self._in_progress.remove(handle)
            self._rescheduled_items.add(handle)
            self._new_items.notify_all()

    async def reschedule_all(self, consumer: "Consumer[Item]"):
        """Make all items currently assigned to the consumer available for reassignment."""
        async with self._lock:
            handles = [handle for handle in self._in_progress if handle.consumer == consumer]
            for handle in handles:
                self._in_progress.remove(handle)
                self._rescheduled_items.add(handle)
            self._new_items.notify_all()

    def stats(self) -> Dict:
        return {
            "locked": self._lock.locked(),
            "in progress": len(self._in_progress),
            "rescheduled": len(self._rescheduled_items),
            "in buffer": self._buffer.qsize(),
            "incoming finished": self._incoming_finished,
        }

    async def wait_until_done(self) -> None:
        """Wait until all items in the queue are processed."""
        async with self._lock:
            while not self.finished():
                await self._eof.wait()


class Consumer(
    Generic[Item],
    AsyncIterator[Handle[Item]],
    AsyncIterable[Handle[Item]],
    ContextManager["Consumer[Item]"],
):
    """
    Provides an interface to asynchronously iterate over items in the given queue
    while cooperating with other consumers attached to this queue.
    """

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
    def current_item(self) -> Optional[Item]:
        """The most-recent queue item that has been fetched to be processed by this consumer."""
        return self._fetched.data if self._fetched else None

    async def __anext__(self) -> Handle[Item]:
        val = await self._queue.get(self)
        self._fetched = val
        return val
