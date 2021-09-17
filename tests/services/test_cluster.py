import asyncio
import itertools
import sys
import pytest
from unittest.mock import Mock, patch, call
from yapapi.services import Cluster, Service, ServiceError, ServiceInstance


class _TestService(Service):
    pass


class _BrokenService(Service):
    async def start(self):
        await asyncio.Future()


def _get_cluster() -> Cluster:
    return Cluster(engine=Mock(), service_class=_TestService, payload=Mock())


@pytest.mark.parametrize(
    "kwargs, calls, error",
    [
        (
            {"num_instances": 1},
            [call({}, None)],
            None,
        ),
        (
            {"num_instances": 3},
            [call({}, None) for _ in range(3)],
            None,
        ),
        (
            {"instance_params": [{}]},
            [call({}, None)],
            None,
        ),
        (
            {"instance_params": [{"n": 1}, {"n": 2}]},
            [call({"n": 1}, None), call({"n": 2}, None)],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 2, "instance_params": [{} for _ in range(3)]},
            [call({}, None), call({}, None)],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 3, "instance_params": ({"n": i} for i in itertools.count(1))},
            [call({"n": 1}, None), call({"n": 2}, None), call({"n": 3}, None)],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 4, "instance_params": [{} for _ in range(3)]},
            [call({}, None) for _ in range(3)],
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
            [call({}, "10.0.0.1"), call({}, "10.0.0.2"), call({}, None)],
            None,
        ),
    ],
)
@pytest.mark.asyncio
@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
async def test_spawn_instances(kwargs, calls, error):
    with patch("yapapi.services.Cluster.spawn_instance") as spawn_instance:
        cluster = _get_cluster()
        try:
            cluster.spawn_instances(**kwargs)
        except ServiceError as e:
            if error is not None:
                assert str(e) == error
            else:
                assert False, e
        else:
            assert error is None, f"Expected ServiceError: {error}"

    assert spawn_instance.mock_calls == calls


@pytest.mark.parametrize(
    "service, error",
    (
        (
            _TestService(Mock(), Mock()),
            None
        ),
        (
            _BrokenService(Mock(), Mock()),
            "must be an asynchronous generator"
        ),
    )
)
def test_get_handler(service, error):
    service_instance = ServiceInstance(service=service)
    try:
        handler = Cluster._get_handler(service_instance)
        assert handler
    except ServiceError as e:
        if error is not None:
            assert error in str(e)
        else:
            assert False, e
    else:
        assert error is None, f"Expected ServiceError containing '{error}'"
