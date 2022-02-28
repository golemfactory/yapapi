from yapapi import events
from typing import Callable


class AsyncEventEmitter:
    def __init__(self):
        self._consumers = []

    def add_event_consumer(self, event_consumer: Callable[[events.Event], None]):
        self._consumers.append(event_consumer)

    def emit(self, event: events.Event) -> None:
        for consumer in self._consumers:
            consumer(event)
