from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable, DefaultDict, Iterable, List, Optional, Tuple, Type

from yapapi.mid.events import Event, ResourceEvent


class EventTemplate(ABC):
    @abstractmethod
    def includes(self, event: Event) -> bool:
        pass


@dataclass(frozen=True)
class AnyEventTemplate(EventTemplate):
    event_classes: Tuple[Type[Event], ...]

    def includes(self, event: Event) -> bool:
        return not self.event_classes or any(isinstance(event, cls) for cls in self.event_classes)


@dataclass(frozen=True)
class ResourceEventTemplate(EventTemplate):
    event_classes: Tuple[Type[ResourceEvent], ...]
    resource_ids: Tuple[str, ...]

    def includes(self, event: Event) -> bool:
        if not isinstance(event, ResourceEvent):
            return False
        class_match = not self.event_classes or any(isinstance(event, cls) for cls in self.event_classes)
        return class_match and \
            not self.resource_ids or event.resource.id in self.resource_ids


class EventBus:
    def __init__(self):
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self.consumers: DefaultDict[EventTemplate, List[Callable[[Event], Awaitable[None]]]] = defaultdict(list)

        self._task: Optional[asyncio.Task] = None

    def start(self):
        assert self._task is None
        self._task = asyncio.create_task(self._emit_events())

    async def stop(self):
        if self._task:
            await self.queue.join()
            self._task.cancel()
            self._task = None

    def resource_listen(
        self,
        callback: Callable[[Event], Awaitable[None]],
        classes: Iterable[Type[ResourceEvent]] = (),
        ids: Iterable[str] = (),
    ) -> None:
        template = ResourceEventTemplate(tuple(classes), tuple(ids))
        self.consumers[template].append(callback)

    def listen(
        self,
        callback: Callable[[Event], Awaitable[None]],
        classes: Iterable[Type[Event]] = (),
    ) -> None:
        template = AnyEventTemplate(tuple(classes))
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
