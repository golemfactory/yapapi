import asyncio

from dataclasses import dataclass

from yapapi.props.builder import DemandBuilder
from yapapi.props.base import prop, constraint
from yapapi.props import inf

from yapapi.payload import Payload


TURBOGETH_RUNTIME_NAME = "turbogeth-managed"
PROP_TURBOGETH_RPC_PORT = "golem.srv.app.eth.rpc-port"


@dataclass
class TurbogethPayload(Payload):
    rpc_port: int = prop(PROP_TURBOGETH_RPC_PORT, None)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, "=", TURBOGETH_RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, ">=", 16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, ">=", 1024)


async def main():
    builder = DemandBuilder()
    await builder.decorate(TurbogethPayload(rpc_port=1234))
    print(builder)


asyncio.run(main())
