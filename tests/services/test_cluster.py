import asyncio
import itertools
import pytest
import sys
from unittest import mock
from unittest.mock import Mock, patch

from yapapi import Golem
from yapapi.services import Service, ServiceRunner


class _TestService(Service):
    def __init__(self, **kwargs):
        super().__init__()
        self.init_kwargs = kwargs


class _BrokenService(Service):
    async def start(self):
        await asyncio.Future()


@pytest.mark.parametrize(
    "kwargs, args, error",
    [
        (
            {"num_instances": 1},
            [({}, None)],
            None,
        ),
        (
            {"num_instances": 3},
            [({}, None) for _ in range(3)],
            None,
        ),
        (
            {"instance_params": [{}]},
            [({}, None)],
            None,
        ),
        (
            {"instance_params": [{"n": 1}, {"n": 2}]},
            [({"n": 1}, None), ({"n": 2}, None)],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 2, "instance_params": [{} for _ in range(3)]},
            [({}, None), ({}, None)],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 3, "instance_params": ({"n": i} for i in itertools.count(1))},
            [({"n": 1}, None), ({"n": 2}, None), ({"n": 3}, None)],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 4, "instance_params": [{} for _ in range(3)]},
            [({}, None) for _ in range(3)],
            "`instance_params` iterable depleted after 3 spawned instances.",
        ),
        (
            {"num_instances": 0},
            [],
            None,
        ),
        (
            {
                "num_instances": 3,
                "network_addresses": [
                    "10.0.0.1",
                    "10.0.0.2",
                ],
            },
            [({}, "10.0.0.1"), ({}, "10.0.0.2"), ({}, None)],
            None,
        ),
    ],
)
@pytest.mark.asyncio
@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
async def test_spawn_instances(kwargs, args, error, monkeypatch):
    def _get_new_engine(self):
        return mock.AsyncMock()

    monkeypatch.setattr(Golem, "_get_new_engine", _get_new_engine)

    with patch("yapapi.services.ServiceRunner.spawn_instance") as spawn_instance:
        golem = Golem(budget=1)
        try:
            await golem.run_service(
                service_class=_TestService, payload=Mock(), network=Mock(), **kwargs
            )
        except ValueError as e:
            if error is not None:
                assert str(e) == error
            else:
                assert False, e
        else:
            assert error is None, f"Expected ServiceError: {error}"

    assert len(spawn_instance.mock_calls) == len(args)
    for call_args, args in zip(spawn_instance.call_args_list, args):
        service, network, network_address = call_args[0]
        assert service.init_kwargs == args[0]
        assert network_address == args[1]


@pytest.mark.parametrize(
    "service, error",
    (
        (_TestService(), None),
        (_BrokenService(), "must be an asynchronous generator"),
    ),
)
def test_get_handler(service, error):
    service.service_instance.service_state.lifecycle()  # pending -> starting
    try:
        handler = ServiceRunner._get_handler(service.service_instance)
        assert handler
    except TypeError as e:
        if error is not None:
            assert error in str(e)
        else:
            assert False, e
    else:
        assert error is None, f"Expected ServiceError containing '{error}'"
