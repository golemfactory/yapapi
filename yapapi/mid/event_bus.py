from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, DefaultDict, Iterable, List, Optional, Tuple, Type

from yapapi.mid.events import Event, ResourceEvent
from yapapi.mid.resource import Resource


######################################
#   GENERAL NOTE ABOUT EVENT FILTERS
#
#   Thanks to the EventFilters, event listeners listening for the same
#   sort of events are kept together. Purpose:
#   *   A little more efficient (less checks to execute on an single event)
#   *   We might want to implement one day some  "this will never happen" logic
#       - e.g. after we deleted a resource, it will never change again. This will not be
#       possible with unstructured callables.
#
#   BUT: this might end up useless - but cleanup will be very easy, as this is internal to the EventBus.
class EventFilter(ABC):
    """Base class for all EventFilters"""
    @abstractmethod
    def includes(self, event: Event) -> bool:
        pass


@dataclass(frozen=True)
class AnyEventFilter(EventFilter):
    """Selection based on the Event classes."""
    event_classes: Tuple[Type[Event], ...]

    def includes(self, event: Event) -> bool:
        return not self.event_classes or any(isinstance(event, cls) for cls in self.event_classes)


@dataclass(frozen=True)
class ResourceEventFilter(EventFilter):
    """ResourceEvents with optional filters by event class/resource type/resource id"""
    event_classes: Tuple[Type[ResourceEvent], ...]
    resource_classes: Tuple[Type[Resource], ...]
    resource_ids: Tuple[str, ...]

    def includes(self, event: Event) -> bool:
        if not isinstance(event, ResourceEvent):
            return False

        event_class_match = not self.event_classes or any(isinstance(event, cls) for cls in self.event_classes)
        if not event_class_match:
            return False

        resource_class_match = not self.resource_classes or \
            any(isinstance(event.resource, cls) for cls in self.resource_classes)
        if not resource_class_match:
            return False

        return not self.resource_ids or event.resource.id in self.resource_ids


class EventBus:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self.consumers: DefaultDict[EventFilter, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)

        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        assert self._task is None
        self._task = asyncio.create_task(self._emit_events())

    async def stop(self) -> None:
        if self._task:
            await self.queue.join()
            self._task.cancel()
            self._task = None

    def resource_listen(
        self,
        callback: Callable[[ResourceEvent], Awaitable[None]],
        event_classes: Iterable[Type[ResourceEvent]] = (),
        resource_classes: Iterable[Type[Resource]] = (),
        ids: Iterable[str] = (),
    ) -> None:
        template = ResourceEventFilter(tuple(event_classes), tuple(resource_classes), tuple(ids))
        self.consumers[template].append(callback)

    def listen(
        self,
        callback: Callable[[Event], Awaitable[None]],
        classes: Iterable[Type[Event]] = (),
    ) -> None:
        template = AnyEventFilter(tuple(classes))
        self.consumers[template].append(callback)

    def emit(self, event: Event) -> None:
        self.queue.put_nowait(event)

    async def _emit_events(self) -> None:
        while True:
            event = await self.queue.get()
            await self._emit(event)

    async def _emit(self, event: Event) -> None:
        tasks = []
        for event_template, callbacks in self.consumers.items():
            if event_template.includes(event):
                tasks += [callback(event) for callback in callbacks]
        if tasks:
            await asyncio.gather(*tasks)
        self.queue.task_done()
