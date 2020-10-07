"""Utility functions and classes used within the `yapapi.runner` package."""
import asyncio
from typing import AsyncContextManager, Callable, Optional


class AsyncWrapper(AsyncContextManager):
    """Wraps a given callable to provide asynchronous calls.

    Example usage:

      with AsyncWrapper(func) as wrapper:
          wrapper.async_call("Hello", world=True)
          wrapper.async_call("Bye!")

    The above code will make two asynchronous calls to `func`.
    The results of the calls, if any, are discarded, so this class is
    most useful for wrapping callables that return `None`.
    """

    _wrapped: Callable
    _args_buffer: asyncio.Queue
    _task: Optional[asyncio.Task]
    _loop: asyncio.AbstractEventLoop

    def __init__(self, wrapped: Callable, event_loop: Optional[asyncio.AbstractEventLoop] = None):
        self._wrapped = wrapped  # type: ignore  # suppress mypy issue #708
        self._args_buffer = asyncio.Queue()
        self._task = None
        self._loop = event_loop or asyncio.get_event_loop()

    async def _worker(self) -> None:
        while True:
            (args, kwargs) = await self._args_buffer.get()
            self._wrapped(*args, **kwargs)
            self._args_buffer.task_done()

    async def __aenter__(self):
        self._task = self._loop.create_task(self._worker())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._args_buffer.join()
        if self._task:
            self._task.cancel()
        self._task = None

    def async_call(self, *args, **kwargs) -> None:
        """Schedule an asynchronous call to the wrapped callable."""
        self._args_buffer.put_nowait((args, kwargs))
