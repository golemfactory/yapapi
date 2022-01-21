import asyncio
import functools
import logging
from typing import Callable, Optional

import aiohttp

import ya_market
import ya_activity
import ya_payment


logger = logging.getLogger(__name__)


def is_intermittent_error(e: Exception) -> bool:
    """Check if `e` indicates an intermittent communication failure such as network timeout."""

    is_timeout_exception = isinstance(e, asyncio.TimeoutError) or (
        isinstance(e, (ya_activity.ApiException, ya_market.ApiException, ya_payment.ApiException))
        and e.status in (408, 504)
    )

    return (
        is_timeout_exception
        or isinstance(e, aiohttp.ServerDisconnectedError)
        # OS error with errno 32 is "Broken pipe"
        or (isinstance(e, aiohttp.ClientOSError) and e.errno == 32)
    )


class SuppressedExceptions:
    """An async context manager for suppressing exceptions satisfying given condition."""

    exception: Optional[Exception]

    def __init__(self, condition: Callable[[Exception], bool], report_exceptions: bool = True):
        self._condition = condition
        self._report_exceptions = report_exceptions
        self.exception = None

    async def __aenter__(self) -> "SuppressedExceptions":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_value and self._condition(exc_value):
            self.exception = exc_value
            if self._report_exceptions:
                logger.debug(
                    "Exception suppressed: %r", exc_value, exc_info=(exc_type, exc_value, traceback)
                )
            return True
        return False


def repeat_on_error(
    max_tries: int,
    condition: Callable[[Exception], bool] = is_intermittent_error,
    interval: float = 1.0,
):
    """Decorate a function to repeat calls up to `max_tries` times when errors occur.

    Only exceptions satisfying the given `condition` will cause the decorated function
    to be retried. All remaining exceptions will fall through.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            """Make at most `max_tries` attempts to call `func`."""

            for try_num in range(1, max_tries + 1):

                if try_num > 1:
                    await asyncio.sleep(interval)

                async with SuppressedExceptions(condition, False) as se:
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
