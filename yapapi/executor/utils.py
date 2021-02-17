"""Utility functions and classes used within the `yapapi.executor` package."""
import asyncio
import logging
from typing import Callable, Optional


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

    def __init__(self, wrapped: Callable, event_loop: Optional[asyncio.AbstractEventLoop] = None):
        self._wrapped = wrapped  # type: ignore  # suppress mypy issue #708
        self._args_buffer = asyncio.Queue()
        loop = event_loop or asyncio.get_event_loop()
        self._task = loop.create_task(self._worker())

    async def _worker(self) -> None:
        while True:
            try:
                (args, kwargs) = await self._args_buffer.get()
                try:
                    self._wrapped(*args, **kwargs)
                finally:
                    self._args_buffer.task_done()
            except KeyboardInterrupt as ke:
                # Don't stop on KeyboardInterrupt, but pass it to the event loop
                logger.debug("Caught KeybordInterrupt in AsyncWrapper's worker task")

                def raise_interrupt(ke_):
                    raise ke_

                asyncio.get_event_loop().call_soon(raise_interrupt, ke)
            except asyncio.CancelledError:
                logger.debug("AsyncWrapper's worker task cancelled")
                break
            except Exception:
                logger.exception("Unhandled exception in wrapped callable")

    async def stop(self) -> None:
        """Stop the wrapper, process queued calls but do not accept any new ones."""
        if self._task:
            # Set self._task to None so we don't accept any more calls in `async_call()`
            worker_task = self._task
            self._task = None
            await self._args_buffer.join()
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

    def async_call(self, *args, **kwargs) -> None:
        """Schedule an asynchronous call to the wrapped callable."""
        if not self._task or self._task.done():
            raise RuntimeError("AsyncWrapper is closed")
        self._args_buffer.put_nowait((args, kwargs))
