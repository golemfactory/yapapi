import asyncio
import functools
import logging

import ya_market
import ya_activity
import ya_payment


logger = logging.getLogger(__name__)


def _is_timeout_exception(e: Exception) -> bool:
    """Check if `e` indicates a client-side or server-side timeout error."""

    # `asyncio.TimeoutError` is raised on client-side timeouts
    if isinstance(e, asyncio.TimeoutError):
        return True

    # `ApiException(408)` is raised on server-side timeouts
    if isinstance(e, (ya_activity.ApiException, ya_market.ApiException, ya_payment.ApiException)):
        return e.status == 408

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
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if _is_timeout_exception(e):
                        repeat = try_num < max_tries
                        msg = f"API call timed out (attempt {try_num}/{max_tries}), "
                        msg += f"retrying in {interval} s" if repeat else "giving up"
                        # Don't print traceback if this was the last attempt, let the caller do it.
                        logger.debug(msg, exc_info=repeat)
                        if repeat:
                            continue
                    raise

        return wrapper

    return decorator
