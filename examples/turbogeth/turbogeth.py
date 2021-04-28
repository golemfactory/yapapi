import asyncio
import json
from datetime import timedelta
import typing

from dataclasses import dataclass
from yapapi.executor.ctx import WorkContext
from yapapi.props.builder import DemandBuilder, AutodecoratingModel
from yapapi.props.base import prop, constraint
from yapapi.props import inf

from yapapi.payload import Payload

from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa


TURBOGETH_RUNTIME_NAME = "turbogeth-managed"
PROP_TURBOGETH_RPC_PORT = "golem.srv.app.eth.rpc-port"


@dataclass
class TurbogethPayload(Payload, AutodecoratingModel):
    rpc_port: int = prop(PROP_TURBOGETH_RPC_PORT, default=None)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=TURBOGETH_RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)


INSTANCES_NEEDED = 1
EXECUTOR_TIMEOUT = timedelta(weeks=100)


class Service:
    """ THIS SHOULD BE PART OF THE API"""
    state: typing.Optional[str] = None
    running = ('new', 'deployed', 'ready', 'shutdown')

    def __init__(self, ctx: WorkContext):
        self.ctx = ctx
        self.state = "new"  # should state correspond with the ActivityState?

    async def on_deploy(self, out: bytes):
        self.state = "deployed"

    async def on_start(self, out: bytes):
        self.state = "ready"

    async def on_new(self):
        self.ctx.deploy(on_deploy=self.on_deploy)
        self.ctx.start(on_start=self.on_start)
        yield self.ctx.commit()

    async def execute_batch(self, batch: Optional[Work]):
        if batch:
            executor.execute(batch)  # some automagic of passing it for execution ;)

    async def run(self):  # some way to pass a signal into `run` ... or some other event handler inside `Service`
        while self.state in self.running:
            _handlers = {
                'new': self.on_new,
                'ready': self.on_ready,
                'shutdown': self.on_shutdown,
            }

            handler = _handlers.get(self.state)
            if handler:
                async for batch in handler():
                    await self.execute_batch(batch)

    async def on_ready(self, *args, **kwargs):
        while True:
            print(f"service {self.ctx.id} running on {self.ctx.provider_name} ... ")
            await asyncio.sleep(10)
            yield None

    async def on_shutdown(self):
        yield None


class TurbogethService(Service):
    def __init__(self, ctx: WorkContext):
        super().__init__(ctx)
        self.credentials = {}

    async def on_deploy(self, out: bytes):
        print("deployed")
        self.credentials = json.loads(out.decode("utf-8"))

    async def on_start(self, out: bytes):
        print("started")

    async def on_shutdown(self):
        self.ctx.download_file("some/service/state", "temp/path")


class Cluster:
    """ THIS SHOULD BE PART OF THE API"""
    def __init__(self, executor: "Executor", service: typing.Type[Service], payload: Payload):
        self.executor = executor
        self.service = service
        self.payload = payload
        self.instances: typing.List[Service] = []

    async def _run_instance(self, ctx: WorkContext):
        instance = Service(ctx)
        self.instances.append(instance)

        print(f"{instance} started")
        await instance.run()
        print(f"{instance} finished")

        # pass `instance` to some loop in the executor

    async def spawn_instance(
            self,
    ):
        act = await self.executor.get_activity(self.payload)
        ctx = WorkContext(act.id)
        await self._run_instance(ctx)


class Executor(typing.AsyncContextManager):
    """ MOCK OF EXECUTOR JUST SO I COULD ILLUSTRATE THE NEW CALL"""
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self) -> "Executor":
        print("start executor")
        return self

    async def __aexit__(self, *exc_info):
        print("stop executor", exc_info)
        return True

    def run_service(
            self,
            service: typing.Type[Service],
            payload: Payload,
            num_instances: int = 1,
    ) -> Cluster:
        cluster = Cluster(executor=self, service=service, payload=payload)
        for i in range(num_instances):
            asyncio.create_task(cluster.spawn_instance())
        return cluster


async def main(subnet_tag, driver=None, network=None):

    payload = TurbogethPayload(rpc_port=8888)

    async with Executor(
        max_workers=INSTANCES_NEEDED,
        budget=10.0,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
        event_consumer=log_summary(log_event_repr),
    ) as executor:
        swarm = executor.run_service(
            TurbogethService,
            payload=payload,
            num_instances=INSTANCES_NEEDED
        )

        while True:
            print(f"{swarm} is running: {swarm.instances}")
            await asyncio.sleep(10)


asyncio.run(main(None))

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
