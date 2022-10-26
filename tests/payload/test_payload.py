from dataclasses import dataclass
import pytest

from yapapi.payload import Payload
from yapapi.props import constraint, inf, prop
from yapapi.props.builder import DemandBuilder


@dataclass
class _FooPayload(Payload):
    port: int = prop("golem.srv.app.foo.port", None)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, "=", "foo")
    min_mem_gib: float = constraint(inf.INF_MEM, ">=", 16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, ">=", 1024)


@pytest.mark.asyncio
async def test_payload():
    builder = DemandBuilder()
    await builder.decorate(_FooPayload(port=1234, min_mem_gib=32))
    assert builder.properties == {"golem.srv.app.foo.port": 1234}
    assert (
        builder.constraints
        == "(&(golem.runtime.name=foo)\n\t(golem.inf.mem.gib>=32)\n\t(golem.inf.storage.gib>=1024))"
    )
