"""Utility functions and classes used within yapapi."""
import asyncio
import enum
import functools
import logging
import warnings
from datetime import datetime, timezone, tzinfo
from typing import AsyncContextManager, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class AsyncWrapper(AsyncContextManager):
    """Wraps a given callable to provide asynchronous calls.

    Example usage:

      with AsyncWrapper(func) as wrapper:
          wrapper.async_call("Hello", world=True)
          wrapper.async_call("Bye!")

      # OR

      wrapper = AsyncWrapper(func)
      wrapper.start()
      wrapper.async_call("Hello", world=True)
      wrapper.async_call("Bye!")
      await wrapper.stop()

    The above code will make two asynchronous calls to `func`.
    The results of the calls, if any, are discarded, so this class is
    most useful for wrapping callables that return `None`.
    """

    _wrapped: Callable
    _args_buffer: asyncio.Queue
    _task: Optional[asyncio.Task]

    def __init__(self, wrapped: Callable):
        self._wrapped = wrapped
        self._args_buffer = asyncio.Queue()
        self._loop = asyncio.get_event_loop()
        self._task = None

    async def __aenter__(self) -> "AsyncWrapper":
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        await self.stop()

        # Don't suppress the exception (if any), so return a non-True value
        return None

    @property
    def closed(self) -> bool:
        return self._task is None

    def start(self):
        if self.closed:
            self._task = self._loop.create_task(self._worker())

    async def stop(self):
        """Stop the wrapper, process queued calls but do not accept any new ones."""
        if self.closed:
            return

        # Set self._task to None so we don't accept any more calls in `async_call()`
        assert self._task  # satisfy `worker_task` type check
        worker_task = self._task
        self._task = None

        await self._args_buffer.join()
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

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
        if self.closed:
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
    return warn_deprecated_msg(warning_msg)


def warn_deprecated_msg(warning_msg) -> None:
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


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.

    Copied verbatim from `distutils` because of deprecation thereof.
    (c) Python Software Foundation, available through the GPL-compatible, PSFL v2.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "off", "0"):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


def utc_now() -> datetime:
    """Get a timezone-aware datetime for _now_."""
    return datetime.now(tz=timezone.utc)


def explode_dict(imploded_dict: Dict, separator=".") -> Dict:
    """Return an exploded dictionary based on path found in keys.

    Example usage:

        assert explode_dict(
            {
                "root_field": 1,
                "nested.field": 2,
                "nested.obj": {
                    "works": "fine",
                },
                "nested.obj.with_array": [
                    "okay!",
                ],
                "even.more.nested.field": 3,
                "arrays.0.are": {
                    "supported": "too",
                },
                "arrays.1": "works fine",
            }
        ) == {
            "root_field": 1,
            "nested": {
                "field": 2,
                "obj": {
                    "works": "fine",
                    "with_array": [
                        "okay!",
                    ],
                },
            },
            "even": {
                "more": {
                    "nested": {
                        "field": 3,
                    },
                },
            },
            "arrays": [
                {
                    "are": {
                        "supported": "too",
                    },
                },
                "works fine",
            ],
        }

    """
    obj: Dict = {}

    for path, value in imploded_dict.items():
        parts = path.split(separator)

        nested_obj: Union[List, Dict] = obj
        for part, next_part in zip(parts, parts[1:]):
            try:
                int(next_part)
            except ValueError:
                is_next_part_number = False
            else:
                is_next_part_number = True

            try:
                part_index = int(part)
            except ValueError:
                assert isinstance(nested_obj, Dict)

                if is_next_part_number:
                    nested_obj = nested_obj.setdefault(part, [])
                else:
                    nested_obj = nested_obj.setdefault(part, {})
            else:
                try:
                    nested_obj = nested_obj[part_index]
                except IndexError:
                    assert isinstance(nested_obj, List)

                    new_nested_obj: Union[List, Dict] = [] if is_next_part_number else {}
                    nested_obj.insert(part_index, new_nested_obj)
                    nested_obj = new_nested_obj

        try:
            last_part = int(parts[-1])
        except ValueError:
            assert isinstance(nested_obj, Dict)
            last_part = parts[-1]
        else:
            assert isinstance(nested_obj, List)

            try:
                nested_obj[last_part]
            except IndexError:
                nested_obj.insert(last_part, None)

        nested_obj[last_part] = value

    return obj
