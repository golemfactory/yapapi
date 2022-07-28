import asyncio

from typing import Any, Callable, Dict, List, Optional


class YagnaEventCollector:
    """Utility class that listens for yagna events on a given endpoint

    and puts them in queue(s).
    NOTE: this has nothing to do with "internal" yapapi events."""
    def __init__(self, func: Callable, func_args: List[Any], func_kwargs: Dict[Any, Any]):
        self.func = func
        self.func_args = func_args
        self.func_kwargs = func_kwargs

        self._task: Optional[asyncio.Task] = None
        self._events: List[Any] = []
        self._queues: List[asyncio.Queue] = []

    async def __aenter__(self) -> "YagnaEventCollector":
        self._task = asyncio.create_task(self._collect_events())
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _collect_events(self) -> None:
        while True:
            events = await self.func(*self.func_args, **self.func_kwargs)
            self._events += events

            for event in events:
                for queue in self._queues:
                    queue.put_nowait(event)

            if not events:
                await asyncio.sleep(0.1)

    def event_queue(self, past_events: bool = True) -> asyncio.Queue:
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._queues.append(queue)

        if past_events:
            for event in self._events:
                queue.put_nowait(event)
        return queue
