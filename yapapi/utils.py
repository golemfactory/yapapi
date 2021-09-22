"""Utility functions and classes used within the `yapapi.executor` package."""
import asyncio
from datetime import datetime, timezone, tzinfo
import enum
import functools
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


class Deprecated(enum.Enum):
    module = "module"
    parameter = "parameter"
    property = "property"


def warn_deprecated(old_name: str, new_name: str, since_version: str, entity: Deprecated) -> None:
    """Log a pre-formatted deprecation warning with given parameters.

    :param old_name: name of the entity being deprecated
    :param new_name: name of the entity to be used in favour of deprecated one
    :param since_version: `yapapi` version in which the old entity was first deprecated
    :param entity: enum value indicating the type of the entity being deprecated (e.g. module)
    """
    warning_msg = (
        f"{entity.value.capitalize()} `{old_name}` is deprecated since version {since_version}, "
        f"please use {entity.value} `{new_name}` instead."
    )
    warnings.filterwarnings("default", category=DeprecationWarning, message=warning_msg)
    warnings.warn(
        warning_msg,
        category=DeprecationWarning,
        stacklevel=2,
    )


def get_local_timezone() -> Optional[tzinfo]:
    return datetime.now(timezone.utc).astimezone().tzinfo


class _AddJobId(logging.LoggerAdapter):
    """A LoggerAdapter that adds the value of the `job_id` keyword param to logged messages."""

    def __init__(self, logger, fmt):
        super().__init__(logger, extra={})
        self.format = fmt

    def process(self, msg, kwargs):
        job_id = kwargs.get("job_id")
        if job_id is not None:
            msg = self.format.format(job_id=job_id, msg=msg)
            del kwargs["job_id"]
        return msg, kwargs


@functools.lru_cache(None)
def get_logger(name: str, fmt="[Job {job_id}] {msg}"):
    """Get named logger instance.

    May be used as replacement for `logging.getLogger()`. The difference is, that
    the returned loggers accept `job_id` keyword argument and include it in the
    formated message. The optional `fmt` parameter specifies how the log message
    and the job_id should be formatted together.
    """
    logger = logging.getLogger(name)
    return _AddJobId(logger, fmt=fmt)
