import asyncio

from dataclasses import dataclass

from yapapi.props.builder import DemandBuilder, AutodecoratingModel
from yapapi.props.base import prop, constraint
from yapapi.props import inf

from yapapi.payload import Payload


TURBOGETH_RUNTIME_NAME = "turbogeth-managed"
PROP_TURBOGETH_RPC_PORT = "golem.srv.app.eth.rpc-port"


@dataclass
class Turbogeth(Payload, AutodecoratingModel):
    rpc_port: int = prop(PROP_TURBOGETH_RPC_PORT, default=None)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=TURBOGETH_RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)


async def main():
    builder = DemandBuilder()
    await builder.decorate(Turbogeth(rpc_port=1234))
    print(builder)


asyncio.run(main())

# notes for next steps
#
# -> Service class
#
# Executor.run_service(Service, num_instance=3)
#
# service instance -> instance of the Service class
#
# Service.shutdown should signal Executor to call Services' `exit`
#
# when "run" finishes, the service shuts down
#
# next: save/restore of the Service state
