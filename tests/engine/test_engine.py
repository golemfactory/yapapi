"""Unit tests for `yapapi.engine` module."""
from unittest.mock import Mock

import pytest

import yapapi.engine
import yapapi.rest
from tests.factories.golem import GolemFactory
from yapapi import Golem
from yapapi.config import ApiConfig
from yapapi.engine import Job


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
        golem = GolemFactory(subnet_tag=subnet_arg)
    else:
        golem = GolemFactory()
    assert golem.subnet_tag == expected_subnet


def test_job_id(monkeypatch):
    """Test automatic generation of job ids."""

    used_ids = []

    job_1 = Job(engine=Mock(), expiration_time=Mock(), payload=Mock())
    assert job_1.id
    used_ids.append(job_1.id)

    job_2 = Job(engine=Mock(), expiration_time=Mock(), payload=Mock())
    assert job_2.id
    assert job_2.id not in used_ids
    used_ids.append(job_2.id)

    user_id_3 = f"{job_1.id}:{job_2.id}"
    job_3 = Job(engine=Mock(), expiration_time=Mock(), payload=Mock(), id=user_id_3)
    assert job_3.id == user_id_3
    used_ids.append(user_id_3)

    job_4 = Job(engine=Mock(), expiration_time=Mock(), payload=Mock())
    assert job_4.id
    assert job_4.id not in used_ids
    used_ids.append(job_4.id)

    # Assuming generated ids are just numbers: pass str(N+1) as the user-specified id,
    # where N is the numeric value of the last autogenerated id, and make sure the next
    # autogenerated id is not str(N+1) (a duplicate).
    numeric_ids = set()
    for id in used_ids:
        try:
            numeric_ids.add(int(id))
        except ValueError:
            pass
    if numeric_ids:
        max_id = max(numeric_ids)
        next_id = str(max_id + 1)
        job_5 = Job(engine=Mock(), expiration_time=Mock(), payload=Mock(), id=next_id)
        used_ids.append(job_5.id)
        job_6 = Job(engine=Mock(), expiration_time=Mock(), payload=Mock())
        assert job_6.id not in used_ids

    # Passing an already used id should raise a ValueError
    with pytest.raises(ValueError):
        duplicate_id = used_ids[0]
        Job(engine=Mock(), expiration_time=Mock(), payload=Mock(), id=duplicate_id)
