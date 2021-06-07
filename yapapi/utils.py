"""Utility functions and classes used within the `yapapi.executor` package."""
import asyncio
import logging
from typing import AsyncContextManager, Callable, Optional
import warnings


logger = logging.getLogger(__name__)


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

    def __init__(self, wrapped: Callable):
        self._wrapped = wrapped  # type: ignore  # suppress mypy issue #708
        self._args_buffer = asyncio.Queue()
        self._loop = asyncio.get_event_loop()
        self._task = None

    async def __aenter__(self) -> "AsyncWrapper":
        self._task = self._loop.create_task(self._worker())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        """Stop the wrapper, process queued calls but do not accept any new ones."""
        if self._task:
            # Set self._task to None so we don't accept any more calls in `async_call()`
            worker_task = self._task
            self._task = None
            await self._args_buffer.join()
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
        # Don't suppress the exception (if any), so return a non-True value
        return None

    async def _worker(self) -> None:
        while True:
            try:
                (args, kwargs) = await self._args_buffer.get()
                try:
                    self._wrapped(*args, **kwargs)
                finally:
                    self._args_buffer.task_done()
                    await asyncio.sleep(0)
            except KeyboardInterrupt as ke:
                # Don't stop on KeyboardInterrupt, but pass it to the event loop
                logger.debug("Caught KeybordInterrupt in AsyncWrapper's worker task")

                def raise_interrupt(ke_):
                    raise ke_

                self._loop.call_soon(raise_interrupt, ke)
            except asyncio.CancelledError:
                logger.debug("AsyncWrapper's worker task cancelled")
                break
            except Exception:
                logger.exception("Unhandled exception in wrapped callable")

    def async_call(self, *args, **kwargs) -> None:
        """Schedule an asynchronous call to the wrapped callable."""
        if not self._task or self._task.done():
            raise RuntimeError("AsyncWrapper is closed")
        self._args_buffer.put_nowait((args, kwargs))


def show_module_deprecation_warning(old_module: str, new_module: str, since_version: str) -> None:

    warnings.filterwarnings("default", category=DeprecationWarning, module=old_module)
    warnings.warn(
        f"Module `{old_module}` is deprecated since version {since_version}, "
        f"please use module `{new_module}` instead",
        category=DeprecationWarning,
        stacklevel=2,
    )
