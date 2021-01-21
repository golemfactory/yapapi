"""Unit tests for code that selects payment platforms based on driver/network specification."""
import os
from unittest import mock

import pytest

from ya_payment import RequestorApi

from yapapi.executor import Executor, NoPaymentAccountError, DEFAULT_NETWORK, DEFAULT_DRIVER
from yapapi.rest.payment import Account, Payment


@pytest.fixture(autouse=True)
def _set_app_key(monkeypatch):
    monkeypatch.setenv("YAGNA_APPKEY", "mock-appkey")


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


class _StopExecutor(Exception):
    """An exception raised to stop the test when reaching an expected checkpoint in executor."""


@pytest.fixture()
def _mock_decorate_demand(monkeypatch):
    """Make `Payment.decorate_demand()` stop the test."""
    monkeypatch.setattr(
        Payment,
        "decorate_demand",
        mock.Mock(side_effect=_StopExecutor("decorate_demand() called")),
    )


@pytest.fixture()
def _mock_create_allocation(monkeypatch):
    """Make `RequestorApi.create_allocation()` stop the test."""
    monkeypatch.setattr(
        RequestorApi,
        "create_allocation",
        mock.Mock(side_effect=_StopExecutor("create_allocation() called")),
    )


@pytest.mark.asyncio
async def test_no_accounts_raises(monkeypatch):
    """Test that exception is raised if `Payment.accounts()` returns empty list."""

    monkeypatch.setattr(Payment, "accounts", _mock_accounts_iterator())

    async with Executor(package=mock.Mock(), budget=10.0) as executor:
        with pytest.raises(NoPaymentAccountError):
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass


@pytest.mark.asyncio
async def test_no_matching_account_raises(monkeypatch):
    """Test that exception is raised if `Payment.accounts()` returns no matching accounts."""

    monkeypatch.setattr(
        Payment,
        "accounts",
        _mock_accounts_iterator(
            ("other-driver", "other-network"),
            ("matching-driver", "other-network"),
            ("other-driver", "matching-network"),
        ),
    )

    async with Executor(
        package=mock.Mock(), budget=10.0, driver="matching-driver", network="matching-network"
    ) as executor:
        with pytest.raises(NoPaymentAccountError) as exc_info:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass
        exc = exc_info.value
        assert exc.required_driver == "matching-driver"
        assert exc.required_network == "matching-network"


@pytest.mark.asyncio
async def test_matching_account_creates_allocation(monkeypatch, _mock_decorate_demand):
    """Test that matching accounts are correctly selected and allocations are created for them."""

    monkeypatch.setattr(
        Payment,
        "accounts",
        _mock_accounts_iterator(
            ("other-driver", "other-network"),
            ("matching-driver", "matching-network", {"platform": "platform-1"}),
            ("matching-driver", "other-network"),
            ("other-driver", "matching-network"),
            ("matching-driver", "matching-network", {"platform": "platform-2"}),
        ),
    )

    create_allocation_args = []

    async def mock_create_allocation(_self, model):
        create_allocation_args.append(model)
        return mock.Mock()

    monkeypatch.setattr(RequestorApi, "create_allocation", mock_create_allocation)

    with pytest.raises(_StopExecutor):
        async with Executor(
            package=mock.Mock(), budget=10.0, driver="matching-driver", network="matching-network",
        ) as executor:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass

    assert len(create_allocation_args) == 2
    assert create_allocation_args[0].payment_platform == "platform-1"
    assert create_allocation_args[1].payment_platform == "platform-2"


@pytest.mark.asyncio
async def test_driver_network_case_insensitive(monkeypatch, _mock_create_allocation):
    """Test that matching driver and network names is not case sensitive."""

    monkeypatch.setattr(Payment, "accounts", _mock_accounts_iterator(("dRIVER", "NetWORK")))

    with pytest.raises(_StopExecutor):
        async with Executor(
            package=mock.Mock(), budget=10.0, driver="dRiVeR", network="NeTwOrK",
        ) as executor:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass


@pytest.mark.asyncio
async def test_default_driver_network(monkeypatch, _mock_create_allocation):
    """Test that defaults are used if driver and network are not specified."""

    monkeypatch.setattr(
        Payment, "accounts", _mock_accounts_iterator((DEFAULT_DRIVER, DEFAULT_NETWORK))
    )

    with pytest.raises(_StopExecutor):
        async with Executor(package=mock.Mock(), budget=10.0) as executor:
            async for _ in executor.submit(worker=mock.Mock(), data=mock.Mock()):
                pass
