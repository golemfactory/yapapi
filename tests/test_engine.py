"""Unit tests for `yapapi.engine` module."""
import importlib
from unittest.mock import Mock

import pytest

from yapapi import Golem
import yapapi.rest


@pytest.fixture(autouse=True)
def mock_rest_configuration(monkeypatch):
    """Mock `yapapi.rest.Configuration`."""
    monkeypatch.setattr(yapapi.rest, "Configuration", Mock)


def test_subnet_tag_env_var_not_set(monkeypatch):
    """Check that `subnet_tag` is `None` if not passed as an argument and not set in the env."""

    monkeypatch.delenv("YAGNA_SUBNET", raising=False)
    importlib.reload(yapapi.engine)

    golem = Golem(budget=1.0)
    assert golem.subnet_tag is None


def test_subnet_tag_env_var_set(monkeypatch):
    """Check that if subnet tag is not passed as an argument then the value from env is used."""

    monkeypatch.setenv("YAGNA_SUBNET", "my-little-subnet")
    importlib.reload(yapapi.engine)

    golem = Golem(budget=1.0)
    assert golem.subnet_tag == "my-little-subnet"


def test_subnet_tag_arg_set(monkeypatch):
    """Check that the value from environment is overriden by the `subnet_tag` argument."""

    monkeypatch.setenv("YAGNA_SUBNET", "my-little-subnet")
    importlib.reload(yapapi.engine)

    golem = Golem(budget=1.0, subnet_tag="whole-golem")
    assert golem.subnet_tag == "whole-golem"
