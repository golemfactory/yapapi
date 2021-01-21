"""Unit tests for code that selects payment platforms based on driver/network specification."""
import os
from unittest import mock

import asynctest
import pytest

from yapapi.executor import Executor, NoPaymentAccountError, DEFAULT_NETWORK, DEFAULT_DRIVER
from yapapi.rest.payment import Account


@pytest.fixture(autouse=True)
def _set_app_key():
    os.environ["YAGNA_APPKEY"] = "mock-appkey"


def _mock_accounts_iterator(*account_specs):
    """Create an iterator over mock `Account` objects.

    `account_specs` should contain pairs `(driver, network)`, where `driver` and `network`
    are strings, or triples `(driver, network, params)` with `driver` and `network` as before
    and `params` a dictionary containing additional keyword arguments for `Account()`.
    """

    async def _mock(*_args):
        for spec in account_specs:
            params = {
                "platform": "mock-platform",
                "address": "mock-address",
                "driver": spec[0],
                "network": spec[1],
                "token": "mock-token",
                "send": True,
                "receive": True,
            }
            if len(spec) == 3:
                params.update(**spec[2])
            yield Account(**params)

    return _mock


@pytest.mark.asyncio
@mock.patch("yapapi.executor.rest.Payment.accounts", _mock_accounts_iterator())
async def test_no_accounts_raises():
    """Test that exception is raised if `Payment.accounts()` returns empty list."""

    async with Executor(package=mock.Mock(), budget=10.0) as executor:
        with pytest.raises(NoPaymentAccountError):
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass


@pytest.mark.asyncio
@mock.patch(
    "yapapi.executor.rest.Payment.accounts",
    _mock_accounts_iterator(
        ("other-driver", "other-network"),
        ("matching-driver", "other-network"),
        ("other-driver", "matching-network"),
    ),
)
async def test_no_matching_account_raises():
    """Test that exception is raised if `Payment.accounts()` returns no matching accounts."""

    async with Executor(
        package=mock.Mock(), budget=10.0, driver="matching-driver", network="matching-network"
    ) as executor:
        with pytest.raises(NoPaymentAccountError) as exc_info:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass
        exc = exc_info.value
        assert exc.required_driver == "matching-driver"
        assert exc.required_network == "matching-network"


class _StopExecutor(Exception):
    """An exception raised to stop the test when reaching an expected checkpoint in executor."""


@pytest.mark.asyncio
@mock.patch(
    "yapapi.executor.rest.Payment.accounts",
    _mock_accounts_iterator(
        ("other-driver", "other-network"),
        ("matching-driver", "matching-network", {"platform": "platform-1"}),
        ("matching-driver", "other-network"),
        ("other-driver", "matching-network"),
        ("matching-driver", "matching-network", {"platform": "platform-2"}),
    ),
)
@mock.patch(
    "yapapi.executor.rest.Payment.decorate_demand",
    mock.Mock(side_effect=_StopExecutor("decorate_demand() called")),
)
async def test_matching_account_creates_allocation():
    """Test that matching accounts are correctly selected and allocations are created for them."""

    with mock.patch(
        "ya_payment.RequestorApi.create_allocation", asynctest.CoroutineMock()
    ) as mock_create_allocation:
        with pytest.raises(_StopExecutor):
            async with Executor(
                package=mock.Mock(),
                budget=10.0,
                driver="matching-driver",
                network="matching-network",
            ) as executor:
                async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                    pass

    assert len(mock_create_allocation.call_args_list) == 2
    assert mock_create_allocation.call_args_list[0].args[0].payment_platform == "platform-1"
    assert mock_create_allocation.call_args_list[1].args[0].payment_platform == "platform-2"


@pytest.mark.asyncio
@mock.patch("yapapi.executor.rest.Payment.accounts", _mock_accounts_iterator(("dRIVER", "NetWORK")))
@mock.patch(
    "ya_payment.RequestorApi.create_allocation",
    mock.Mock(side_effect=_StopExecutor("create_allocation() called")),
)
async def test_driver_network_case_insensitive():
    """Test that matching driver and network names is not case sensitive."""

    with pytest.raises(_StopExecutor):
        async with Executor(
            package=mock.Mock(), budget=10.0, driver="dRiVeR", network="NeTwOrK",
        ) as executor:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass


@pytest.mark.asyncio
@mock.patch(
    "yapapi.executor.rest.Payment.accounts",
    _mock_accounts_iterator((DEFAULT_DRIVER, DEFAULT_NETWORK)),
)
@mock.patch(
    "ya_payment.RequestorApi.create_allocation",
    mock.Mock(side_effect=_StopExecutor("create_allocation() called")),
)
async def test_default_driver_network():
    """Test that defaults are used if driver and network are not specified."""

    with pytest.raises(_StopExecutor):
        async with Executor(package=mock.Mock(), budget=10.0) as executor:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass
