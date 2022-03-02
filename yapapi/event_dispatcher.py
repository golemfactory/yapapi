import asyncio
from typing import Callable, Dict, Set, Type

from yapapi import events
from yapapi.utils import AsyncWrapper


class AsyncEventDispatcher:
    def __init__(self):
        self._consumers: Dict[AsyncWrapper, Set[Type[events.Event]]] = {}

    def add_event_consumer(
        self,
        event_consumer: Callable[[events.Event], None],
        event_classes: Set[Type[events.Event]],
        start_consumer: bool,
    ):
        consumer = AsyncWrapper(event_consumer)
        if start_consumer:
            consumer.start()
        self._consumers[consumer] = event_classes

    def emit(self, event: events.Event):
        for consumer, event_classes in self._consumers.items():
            if any(isinstance(event, event_cls) for event_cls in event_classes):
                consumer.async_call(event)

    def start(self):
        for consumer in self._consumers:
            if consumer.closed:
                consumer.start()

    async def stop(self):
        if self._consumers:
            await asyncio.gather(*(aw.stop() for aw in self._consumers))
