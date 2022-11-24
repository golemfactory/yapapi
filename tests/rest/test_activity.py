import pytest
from typing import List, Optional, Tuple, Type
from unittest.mock import Mock

from ya_activity.exceptions import ApiException

from yapapi.rest.activity import BatchError, PollingBatch

GetExecBatchResultsSpec = Tuple[Optional[Exception], List[str]]


def mock_activity(specs: List[GetExecBatchResultsSpec]):
    """Create a mock activity.

    The argument `specs` is a list of pairs specifying the behavior of subsequent calls
    to `get_exec_batch_results()`: i-th pair corresponds to the i-th call.
    The first element of the pair is an optional error raised by the call, the second element
    is the activity state (the `.state` component of the object returned by `Activity.state()`).
    """
    i = -1

    async def mock_results(*_args, **_kwargs):
        nonlocal specs, i
        i += 1
        error = specs[i][0]
        if error:
            raise error
        return [Mock(index=0)]

    async def mock_state():
        nonlocal specs, i
        state = specs[i][1]
        return Mock(state=state)

    return Mock(state=mock_state, _api=Mock(get_exec_batch_results=mock_results))


GSB_ERROR = ":( GSB error: some endpoint address not found :("


@pytest.mark.parametrize(
    "specs, expected_error",
    [
        # No errors
        ([(None, ["Running", "Running"])], None),
        # Exception other than ApiException should stop iteration over batch results
        (
            [(ValueError("!?"), ["Running", "Running"])],
            ValueError,
        ),
        # ApiException not related to GSB should stop iteration over batch results
        (
            [(ApiException(status=400), ["Running", "Running"])],
            ApiException,
        ),
        # As above, but with status 500
        (
            [
                (
                    ApiException(http_resp=Mock(status=500, data='{"message": "???"}')),
                    ["Running", "Running"],
                )
            ],
            ApiException,
        ),
        # ApiException not related to GSB should raise BatchError if activity is terminated
        (
            [
                (
                    ApiException(http_resp=Mock(status=500, data='{"message": "???"}')),
                    ["Running", "Terminated"],
                )
            ],
            BatchError,
        ),
        # GSB-related ApiException should cause retrying if the activity is running
        (
            [
                (
                    ApiException(http_resp=Mock(status=500, data=f'{{"message": "{GSB_ERROR}"}}')),
                    ["Running", "Running"],
                ),
                (None, ["Running", "Running"]),
            ],
            None,
        ),
        # As above, but max number of tries is reached
        (
            [
                (
                    ApiException(http_resp=Mock(status=500, data=f'{{"message": "{GSB_ERROR}"}}')),
                    ["Running", "Running"],
                )
            ]
            * PollingBatch.GET_EXEC_BATCH_RESULTS_MAX_TRIES,
            ApiException,
        ),
        # GSB-related ApiException should raise BatchError if activity is terminated
        (
            [
                (
                    ApiException(http_resp=Mock(status=500, data=f'{{"message": "{GSB_ERROR}"}}')),
                    ["Running", "Terminated"],
                )
            ],
            BatchError,
        ),
    ],
)
@pytest.mark.asyncio
async def test_polling_batch_on_gsb_error(
    specs: List[GetExecBatchResultsSpec], expected_error: Optional[Type[Exception]]
) -> None:
    """Test the behavior of PollingBatch when get_exec_batch_results() raises exceptions."""

    PollingBatch.GET_EXEC_BATCH_RESULTS_INTERVAL = 0.1

    activity = mock_activity(specs)
    batch = PollingBatch(activity, "batch_id", 1)
    try:
        async for _ in batch:
            pass
        assert expected_error is None
    except Exception as error:
        assert expected_error is not None and isinstance(error, expected_error)
