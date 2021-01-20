"""Unit tests for code that selects payment platforms based on driver/network specification."""
import os
from unittest import mock

import pytest
import ya_payment

import yapapi
from yapapi.executor import Executor, NoPaymentAccountError, DEFAULT_NETWORK, DEFAULT_DRIVER
from yapapi.rest.payment import Account


@pytest.fixture(autouse=True)
def _set_app_key():
    os.environ["YAGNA_APPKEY"] = "mock-appkey"


def _mock_account(driver: str, network: str, **kwargs) -> Account:
    """Create mock `Account` instance."""

    params = {
        "platform": "mock-platform",
        "address": "mock-address",
        "driver": driver,
        "network": network,
        "token": "mock-token",
        "send": True,
        "receive": True,
    }
    params.update(kwargs)
    return Account(**params)


def _mock_rest_accounts(accounts):
    """Create mock for `Payment.accounts()`"""

    async def _mock(*_args):
        for a in accounts:
            yield a

    return _mock


@pytest.mark.asyncio
async def test_no_accounts_raises(monkeypatch):
    """Test that exception is raised if `Payment.accounts()` returns empty list."""

    monkeypatch.setattr(yapapi.executor.rest.Payment, "accounts", _mock_rest_accounts([]))

    async with Executor(package=mock.MagicMock(), budget=10.0) as executor:
        with pytest.raises(NoPaymentAccountError):
            async for _ in executor.submit(worker=mock.MagicMock(), data=mock.MagicMock()):
                pass


@pytest.mark.asyncio
async def test_no_matching_account_raises(monkeypatch):
    """Test that exception is raised if `Payment.accounts()` returns no matching accounts."""

    monkeypatch.setattr(
        yapapi.executor.rest.Payment,
        "accounts",
        _mock_rest_accounts(
            [
                _mock_account("other-driver", "other-network"),
                _mock_account("matching-driver", "other-network"),
                _mock_account("other-driver", "matching-network"),
            ]
        ),
    )

    async with Executor(
        package=mock.MagicMock(), budget=10.0, driver="matching-driver", network="matching-network"
    ) as executor:
        with pytest.raises(NoPaymentAccountError) as exc_info:
            async for _ in executor.submit(worker=mock.MagicMock(), data=mock.MagicMock()):
                pass
        exc = exc_info.value
        assert exc.required_driver == "matching-driver"
        assert exc.required_network == "matching-network"


class _StopExecutor(Exception):
    """An exception raised to stop the test when reaching an expected checkpoint in executor."""


@pytest.mark.asyncio
async def test_matching_account_creates_allocation(monkeypatch):
    """Test that matching accounts are correctly selected and allocations are created for them."""

    monkeypatch.setattr(
        yapapi.executor.rest.Payment,
        "accounts",
        _mock_rest_accounts(
            [
                _mock_account("other-driver", "other-network"),
                _mock_account("matching-driver", "matching-network", platform="platform-1"),
                _mock_account("matching-driver", "other-network"),
                _mock_account("other-driver", "matching-network"),
                _mock_account("matching-driver", "matching-network", platform="platform-2"),
            ]
        ),
    )

    create_allocation_args = []

    async def _mock_create_allocation(_self, model):
        create_allocation_args.append(model)
        return mock.MagicMock()

    async def _mock_decorate_demand(*args):
        # We're fine, no need to test further
        raise _StopExecutor("decorate_demand called")

    monkeypatch.setattr(ya_payment.RequestorApi, "create_allocation", _mock_create_allocation)
    monkeypatch.setattr(yapapi.executor.rest.Payment, "decorate_demand", _mock_decorate_demand)

    with pytest.raises(_StopExecutor):
        async with Executor(
            package=mock.MagicMock(),
            budget=10.0,
            driver="matching-driver",
            network="matching-network",
        ) as executor:
            async for _ in executor.submit(worker=mock.MagicMock(), data=mock.MagicMock()):
                pass

    assert len(create_allocation_args) == 2
    assert create_allocation_args[0].payment_platform == "platform-1"
    assert create_allocation_args[1].payment_platform == "platform-2"


@pytest.mark.asyncio
async def test_driver_network_case_insensitive(monkeypatch):
    """Test that matching driver and network names is not case sensitive."""

    monkeypatch.setattr(
        yapapi.executor.rest.Payment,
        "accounts",
        _mock_rest_accounts([_mock_account("dRIVER", "NetWORK")]),
    )

    async def _mock_create_allocation(*_args):
        raise _StopExecutor("create_allocation called")

    monkeypatch.setattr(ya_payment.RequestorApi, "create_allocation", _mock_create_allocation)

    with pytest.raises(_StopExecutor):
        async with Executor(
            package=mock.MagicMock(), budget=10.0, driver="dRiVeR", network="NeTwOrK",
        ) as executor:
            async for _ in executor.submit(worker=mock.MagicMock(), data=mock.MagicMock()):
                pass


@pytest.mark.asyncio
async def test_default_driver_network(monkeypatch):
    """Test that defaults are used if driver and network are not specified."""

    monkeypatch.setattr(
        yapapi.executor.rest.Payment,
        "accounts",
        _mock_rest_accounts([_mock_account(DEFAULT_DRIVER, DEFAULT_NETWORK)]),
    )

    async def _mock_create_allocation(*_args):
        raise _StopExecutor("create_allocation called")

    monkeypatch.setattr(ya_payment.RequestorApi, "create_allocation", _mock_create_allocation)

    with pytest.raises(_StopExecutor):
        async with Executor(package=mock.MagicMock(), budget=10.0) as executor:
            async for _ in executor.submit(worker=mock.MagicMock(), data=mock.MagicMock()):
                pass
