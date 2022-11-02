import asyncio
import pytest
from statemachine import State
import sys
from typing import Optional
from unittest import mock

from ya_activity.exceptions import ApiException

from yapapi.ctx import WorkContext
from yapapi.services.service import Service, ServiceState
from yapapi.services.service_runner import ServiceRunner, ServiceRunnerError


def mock_service(init_state: Optional[State] = None):
    service = Service()
    if init_state:
        service.service_instance.service_state = ServiceState(start_value=init_state.name)
    service._ctx = WorkContext(mock.AsyncMock(), mock.Mock(), mock.Mock(), mock.Mock())
    return service


@pytest.mark.asyncio
async def test_ensure_alive_no_interval():
    with mock.patch("asyncio.Future", mock.AsyncMock()) as future:
        service_runner = ServiceRunner(mock.Mock(), health_check_interval=None)
        await service_runner._ensure_alive(mock.AsyncMock())

    future.assert_awaited()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
@pytest.mark.parametrize(
    "service_state, side_effect, num_retries, expected_alive, expected_num_calls",
    (
        (ServiceState.pending, None, None, True, 0),
        (ServiceState.running, None, None, True, 10),
        (ServiceState.running, ApiException(), None, False, 3),
        (ServiceState.running, ApiException(), 2, False, 2),
        (ServiceState.running, ApiException(), 5, False, 5),
    ),
)
@pytest.mark.asyncio
async def test_ensure_alive(
    service_state,
    side_effect,
    num_retries,
    expected_alive,
    expected_num_calls,
):
    service = mock_service(service_state)
    with mock.patch(
        "yapapi.ctx.WorkContext.get_raw_state", mock.AsyncMock(side_effect=side_effect)
    ) as grs_mock:

        service_runner = ServiceRunner(
            mock.Mock(),
            health_check_interval=0.001,
            **({"health_check_retries": num_retries} if num_retries else {}),
        )

        loop = asyncio.get_event_loop()
        ensure_alive = loop.create_task(service_runner._ensure_alive(service))
        sentinel = loop.create_task(asyncio.sleep(0.02))

        done, pending = await asyncio.wait(
            (
                ensure_alive,
                sentinel,
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )

    if expected_alive:
        assert sentinel in done
        assert ensure_alive in pending
    else:
        assert sentinel in pending
        assert ensure_alive in done

        with pytest.raises(ServiceRunnerError):
            ensure_alive.result()

    sentinel.cancel()
    ensure_alive.cancel()

    assert len(grs_mock.mock_calls) >= expected_num_calls
