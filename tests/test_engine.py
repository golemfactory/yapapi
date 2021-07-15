"""Unit tests for `yapapi.engine` module."""
from unittest.mock import Mock

import pytest

from yapapi import Golem
import yapapi.engine
import yapapi.rest


@pytest.fixture(autouse=True)
def mock_rest_configuration(monkeypatch):
    """Mock `yapapi.rest.Configuration`."""
    monkeypatch.setattr(yapapi.rest, "Configuration", Mock)


@pytest.mark.parametrize(
    "default_subnet, subnet_arg, expected_subnet",
    [
        (None, None, None),
        ("my-little-subnet", None, "my-little-subnet"),
        (None, "whole-golem", "whole-golem"),
        ("my-little-subnet", "whole-golem", "whole-golem"),
    ],
)
def test_set_subnet_tag(default_subnet, subnet_arg, expected_subnet, monkeypatch):
    """Check that `subnet_tag` argument takes precedence over `yapapi.engine.DEFAULT_SUBNET`."""

    monkeypatch.setattr(yapapi.engine, "DEFAULT_SUBNET", default_subnet)

    if subnet_arg is not None:
        golem = Golem(budget=1.0, subnet_tag=subnet_arg)
    else:
        golem = Golem(budget=1.0)
    assert golem.subnet_tag is expected_subnet
