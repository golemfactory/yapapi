import asyncio

import pytest

import ya_activity
import ya_market
import ya_payment

from yapapi.rest.common import repeat_on_timeout


@pytest.mark.parametrize(
    "max_tries, exceptions, calls_expected, expected_error",
    [
        (1, [], 1, None),
        (1, [asyncio.TimeoutError()], 1, asyncio.TimeoutError),
        (1, [ya_activity.ApiException(408)], 1, ya_activity.ApiException),
        (1, [ya_activity.ApiException(500)], 1, ya_activity.ApiException),
        (1, [ValueError()], 1, ValueError),
        #
        (2, [], 1, None),
        (2, [asyncio.TimeoutError()], 2, None),
        (2, [ya_activity.ApiException(408)], 2, None),
        (2, [ya_market.ApiException(408)], 2, None),
        (2, [ya_payment.ApiException(408)], 2, None),
        (2, [ya_activity.ApiException(500)], 1, ya_activity.ApiException),
        (2, [ValueError()], 1, ValueError),
        (2, [asyncio.TimeoutError()] * 2, 2, asyncio.TimeoutError),
        #
        (3, [], 1, None),
        (3, [asyncio.TimeoutError()], 2, None),
        (3, [ya_activity.ApiException(408)], 2, None),
        (3, [asyncio.TimeoutError()] * 2, 3, None),
        (3, [asyncio.TimeoutError()] * 3, 3, asyncio.TimeoutError),
        (3, [ya_activity.ApiException(500)], 1, ya_activity.ApiException),
        (3, [asyncio.TimeoutError(), ValueError()], 2, ValueError),
    ],
)
@pytest.mark.asyncio
async def test_repeat_on_timeout(max_tries, exceptions, calls_expected, expected_error):

    calls_made = 0

    @repeat_on_timeout(max_tries=max_tries)
    async def request():
        nonlocal calls_made, exceptions
        calls_made += 1
        if exceptions:
            e = exceptions[0]
            exceptions = exceptions[1:]
            raise e
        return True

    try:
        await request()
    except Exception as e:
        assert expected_error is not None, f"Unexpected exception: {e}"
        assert isinstance(e, expected_error), f"Expected an {expected_error}, got {e}"
    assert calls_made == calls_expected, "{calls_made} attempts were made, expected {num_calls}"
