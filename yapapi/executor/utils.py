"""Utility functions and classes used within the `yapapi.executor` package."""
import asyncio
import logging
from typing import Callable, Generic, Iterable, Iterator, Optional, TypeVar


logger = logging.getLogger(__name__)


class AsyncWrapper:
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
        self._task = self._loop.create_task(self._worker())

    async def _worker(self) -> None:
        try:
            while True:
                (args, kwargs) = await self._args_buffer.get()
                self._wrapped(*args, **kwargs)
                self._args_buffer.task_done()
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.debug("Worker task interrupted", exc_info=True)
        except Exception:
            logger.debug("Unexpected error in worker task", exc_info=True)

    async def stop(self) -> None:
        await self._args_buffer.join()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
            logger.debug("AsyncWrapper stopped")

    def async_call(self, *args, **kwargs) -> None:
        """Schedule an asynchronous call to the wrapped callable."""
        if not self._task:
            raise RuntimeError("AsyncWrapper is closed")
        self._args_buffer.put_nowait((args, kwargs))


E = TypeVar("E")


class LookaheadIterator(Generic[E]):
    """A wrapper iterator that adds `first` and `empty` properties to the wrapped iterator."""

    def __init__(self, base: Iterable[E]):
        self._base: Optional[Iterator[E]] = iter(base)
        self._first: Optional[E] = None
        self._forward()

    def _forward(self) -> None:
        if self.empty:
            return
        assert self._base
        try:
            self._first = next(self._base)
        except StopIteration:
            self._first = None
            self._base = None

    @property
    def empty(self) -> bool:
        """Return `True` iff this iterator is empty."""
        return self._base is None

    @property
    def first(self) -> E:
        """Return the first element if there is any, otherwise raise `StopIteration`."""
        if self.empty:
            raise StopIteration
        return self._first  # type: ignore

    def __next__(self) -> E:
        first = self.first
        self._forward()
        return first

    def __iter__(self) -> Iterator[E]:
        return self
