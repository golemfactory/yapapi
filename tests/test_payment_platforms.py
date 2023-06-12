"""Unit tests for code that selects payment platforms based on driver/network specification."""
from unittest import mock

import pytest

from ya_payment import RequestorApi

from tests.factories.golem import GolemFactory
from yapapi.engine import DEFAULT_DRIVER, DEFAULT_NETWORK, MAINNET_TOKEN_NAME, TESTNET_TOKEN_NAME
from yapapi.golem import Golem, _Engine


@pytest.fixture(autouse=True)
def _set_app_key(monkeypatch):
    monkeypatch.setenv("YAGNA_APPKEY", "mock-appkey")


class _StopExecutor(Exception):
    """An exception raised to stop the test when reaching an expected checkpoint in executor."""


@pytest.fixture()
def _mock_engine_id(monkeypatch):
    """Mock Engine `id`."""

    async def _id(_):
        return

    monkeypatch.setattr(
        _Engine,
        "_id",
        _id,
    )


@pytest.fixture()
def _mock_create_allocation(monkeypatch):
    """Make `RequestorApi.create_allocation()` stop the test."""

    create_allocation_mock = mock.Mock(side_effect=_StopExecutor("create_allocation() called"))

    monkeypatch.setattr(RequestorApi, "create_allocation", create_allocation_mock)

    return create_allocation_mock


@pytest.mark.asyncio
async def test_default(_mock_engine_id, _mock_create_allocation):
    """Test the allocation defaults."""

    with pytest.raises(_StopExecutor):
        async with GolemFactory():
            pass

    assert _mock_create_allocation.called
    assert (
        _mock_create_allocation.mock_calls[0][1][0].payment_platform
        == f"{DEFAULT_DRIVER}-{DEFAULT_NETWORK}-{TESTNET_TOKEN_NAME}"
    )


@pytest.mark.asyncio
async def test_mainnet(_mock_engine_id, _mock_create_allocation):
    """Test the allocation for a mainnet account."""

    with pytest.raises(_StopExecutor):
        async with Golem(budget=10.0, payment_driver="somedriver", payment_network="mainnet"):
            pass

    assert _mock_create_allocation.called
    assert (
        _mock_create_allocation.mock_calls[0][1][0].payment_platform
        == f"somedriver-mainnet-{MAINNET_TOKEN_NAME}"
    )


@pytest.mark.asyncio
async def test_testnet(_mock_engine_id, _mock_create_allocation):
    """Test the allocation for a mainnet account."""

    with pytest.raises(_StopExecutor):
        async with Golem(budget=10.0, payment_driver="somedriver", payment_network="othernet"):
            pass

    assert _mock_create_allocation.called
    assert (
        _mock_create_allocation.mock_calls[0][1][0].payment_platform
        == f"somedriver-othernet-{TESTNET_TOKEN_NAME}"
    )
