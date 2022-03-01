import asyncio
from typing import Callable, List

from yapapi import events
from yapapi.utils import AsyncWrapper


class AsyncEventEmitter:
    def __init__(self):
        self._consumers: List[AsyncWrapper] = []

    def add_event_consumer(self, event_consumer: Callable[[events.Event], None]):
        consumer = AsyncWrapper(event_consumer)
        consumer.start()
        self._consumers.append(consumer)

    def emit(self, event: events.Event):
        for consumer in self._consumers:
            consumer.async_call(event)

    def start(self):
        for consumer in self._consumers:
            if consumer.closed:
                consumer.start()

    async def stop(self):
        if self._consumers:
            await asyncio.gather(*(aw.stop() for aw in self._consumers))
