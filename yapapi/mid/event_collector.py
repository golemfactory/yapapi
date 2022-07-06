import asyncio

from typing import Any, Awaitable, Dict, List, Optional


class EventCollector:
    def __init__(self, func: Awaitable, func_args: List[Any], func_kwargs: Dict[Any, Any]):
        self.func = func
        self.func_args = func_args
        self.func_kwargs = func_kwargs

        self._task: Optional[asyncio.Task] = None
        self._events: List[Any] = []
        self._queues: List[asyncio.Queue] = []

    def start(self):
        assert self._task is None
        self._task = asyncio.create_task(self._collect_events())

    async def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None

    async def _collect_events(self):
        while True:
            events = await self.func(*self.func_args, **self.func_kwargs)
            self._events += events

            for event in events:
                for queue in self._queues:
                    queue.put_nowait(event)

            if not events:
                await asyncio.sleep(1)

    def event_queue(self, past_events=True):
        queue = asyncio.Queue()
        self._queues.append(queue)

        if past_events:
            for event in self._events:
                queue.put_nowait(event)
        return queue
