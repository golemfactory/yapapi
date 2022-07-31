from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, DefaultDict, Iterable, List, Optional, Tuple, Type, TypeVar

from yapapi.mid import events
from yapapi.mid.events import Event, ResourceEvent
from yapapi.mid.resource import Resource

EventType = TypeVar("EventType", bound=events.Event)
ResourceEventType = TypeVar("ResourceEventType", bound=events.ResourceEvent)


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
    """Emit events, listen for events.

    This class has few purposes:

    * Easy monitoring of the execution process (just log all events)
    * Convenient communication between separated parts of the code. E.g. we might want to act on incoming
      invoices from a component that is not connected to any other part of the code - with EventBus,
      we only have to register a callback for NewResource events.
    * Using a producer-consumer pattern to implement parts of the app-specific logic.

    Sample usage::

        async def on_allocation_event(event: ResourceEvent) -> None:
            print(f"Something happened to an allocation!", event)

        golem = GolemNode()
        event_bus: EventBus = golem.event_bus
        event_bus.resource_listen(on_allocation_event, resource_classes=[Allocation])

        async with golem:
            #   This will cause execution of on_allocation_event with a NewResource event
            allocation = await golem.create_allocation(1)
        #   Allocation was created with autoclose=True, so now we also got a ResourceClosed event
    """
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

    def listen(
        self,
        callback: Callable[[EventType], Awaitable[None]],
        classes: Iterable[Type[Event]] = (),
    ) -> None:
        """Execute the callback when :any:`Event` is emitted.

        :param callback: An async function to be executed.
        :param classes: A list of :any:`Event` subclasses - if not empty,
            `callback` will only be executed on events of matching classes.
        """
        template = AnyEventFilter(tuple(classes))
        self.consumers[template].append(callback)

    def resource_listen(
        self,
        callback: Callable[[ResourceEventType], Awaitable[None]],
        event_classes: Iterable[Type[ResourceEvent]] = (),
        resource_classes: Iterable[Type[Resource]] = (),
        ids: Iterable[str] = (),
    ) -> None:
        """Execute the callback when :any:`ResourceEvent` is emitted.

        :param callback: An async function to be executed.
        :param event_classes: A list of :any:`ResourceEvent` subclasses - if not empty,
            `callback` will only be executed only on events of matching classes.
        :param resource_classes: A list of :class:`~yapapi.mid.resource.Resource` subclasses - if not empty,
            `callback` will only be executed on events related to resources of a matching class.
        :param ids: A list of resource IDs - if not empty,
            `callback` will only be executed on events related to resources with a matching ID.
        """
        template = ResourceEventFilter(tuple(event_classes), tuple(resource_classes), tuple(ids))
        self.consumers[template].append(callback)

    def emit(self, event: Event) -> None:
        """Emit an event - execute all callbacks listening for matching events.

        If emit(X) was called before emit(Y), then it is guaranteed that callbacks
        for event Y will start only after all X callbacks finished (TODO - this should change,
        we don't want the EventBus to stop because of a single never-ending callback).

        :param event: An event that will be emitted.
        """
        self.queue.put_nowait(event)

    async def _emit_events(self) -> None:
        while True:
            event = await self.queue.get()
            await self._emit(event)

    async def _emit(self, event: Event) -> None:
        #   TODO: With this implementation a single never-ending callback will stop
        #         the whole EventBus. This should be fixed in the future.
        tasks = []
        for event_template, callbacks in self.consumers.items():
            if event_template.includes(event):
                tasks += [callback(event) for callback in callbacks]
        if tasks:
            await asyncio.gather(*tasks)
        self.queue.task_done()
