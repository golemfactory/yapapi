import itertools
import pytest
from unittest.mock import Mock, patch, call, AsyncMock
from yapapi.services import Cluster, Service, ServiceError


class _TestService(Service):
    pass


def _get_cluster():
    return Cluster(engine=Mock(), service_class=_TestService, payload=Mock())


@pytest.mark.parametrize(
    "kwargs, calls, error",
    [
        (
            {"num_instances": 1},
            [call({})],
            None,
        ),
        (
            {"num_instances": 3},
            [call({}) for _ in range(3)],
            None,
        ),
        (
            {"instance_params": [{}]},
            [call({})],
            None,
        ),
        (
            {"instance_params": [{"n": 1}, {"n": 2}]},
            [call({"n": 1}), call({"n": 2})],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 2, "instance_params": [{} for _ in range(3)]},
            [call({}), call({})],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 3, "instance_params": ({"n": i} for i in itertools.count(1))},
            [call({"n": 1}), call({"n": 2}), call({"n": 3})],
            None,
        ),
        (
            # num_instances takes precedence
            {"num_instances": 4, "instance_params": [{} for _ in range(3)]},
            [call({}) for _ in range(3)],
            "`instance_params` iterable depleted after 3 spawned instances.",
        ),
    ],
)
@pytest.mark.asyncio
async def test_spawn_instances(kwargs, calls, error):
    with patch("yapapi.services.Cluster.spawn_instance", AsyncMock()) as spawn_instance:
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
