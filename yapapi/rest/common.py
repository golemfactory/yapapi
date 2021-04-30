import asyncio
import contextlib
import functools
import logging
from typing import Callable, Optional

import aiohttp

import ya_market
import ya_activity
import ya_payment


logger = logging.getLogger(__name__)


def is_timeout_exception(e: Exception) -> bool:
    """Check if `e` indicates a client-side or server-side timeout error."""

    # `asyncio.TimeoutError` is raised on client-side timeouts
    if isinstance(e, asyncio.TimeoutError):
        return True

    # `ApiException(408)` is raised on server-side timeouts
    if isinstance(e, (ya_activity.ApiException, ya_market.ApiException, ya_payment.ApiException)):
        return e.status == 408

    return False


def is_recoverable_exception(e: Exception) -> bool:
    """Check if `e` indicates that we should retry the operation that raised it."""

    return is_timeout_exception(e) or isinstance(e, aiohttp.ServerDisconnectedError)


class SuppressedExceptions(contextlib.AbstractAsyncContextManager):
    """An async context manager for suppressing exception satisfying given test function."""

    exception: Optional[Exception]

    def __init__(
        self, should_suppress: Callable[[Exception], bool], report_exceptions: bool = True
    ):
        self._should_suppress = should_suppress
        self._report_exceptions = report_exceptions
        self.exception = None

    async def __aenter__(self) -> "SuppressedExceptions":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_value and self._should_suppress(exc_value):
            self.exception = exc_value
            if self._report_exceptions:
                logger.debug(
                    "Exception suppressed: %r", exc_value, exc_info=(exc_type, exc_value, traceback)
                )
            return True
        return False


def repeat_on_timeout(max_tries: int, interval: float = 0.0):
    """Decorate a function to repeat calls up to `max_tries` times when timeout errors occur."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            """Make at most `max_tries` attempts to call `func`."""

            for try_num in range(1, max_tries + 1):

                if try_num > 1:
                    await asyncio.sleep(interval)

                async with SuppressedExceptions(is_timeout_exception, False) as se:
                    return await func(*args, **kwargs)

                assert se.exception  # noqa (unreachable)
                repeat = try_num < max_tries
                msg = f"API call timed out (attempt {try_num}/{max_tries}), "
                msg += f"retrying in {interval} s" if repeat else "giving up"
                # Don't print traceback if this was the last attempt, let the caller do it.
                logger.debug(msg, exc_info=repeat)
                if not repeat:
                    raise se.exception

        return wrapper

    return decorator
