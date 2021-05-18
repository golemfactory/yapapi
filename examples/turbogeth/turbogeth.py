import asyncio
import enum
from datetime import timedelta
import typing
from typing import Optional, Any
import random

from dataclasses import dataclass, field

from yapapi.props.builder import DemandBuilder
from yapapi.props.base import prop, constraint
from yapapi.props import inf

from yapapi.payload import Payload

from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa


###############################################################
#                                                             #
#                                                             #
#                         MOCK API CODE                       #
#                                                             #
#                                                             #
###############################################################

class Golem(typing.AsyncContextManager):
    """ MOCK OF EXECUTOR JUST SO I COULD ILLUSTRATE THE NEW CALL"""

    def __init__(self, *args, **kwargs):
        print(f"Golem started with {args}, {kwargs}")

    async def __aenter__(self) -> "Golem":
        print("start executor")
        return self

    async def __aexit__(self, *exc_info):
        import traceback

        tb = traceback.print_tb(exc_info[2]) if len(exc_info) > 2 else None
        print("stop executor", exc_info, tb)
        return True

    async def get_activity(self, payload):
        builder = DemandBuilder()
        await builder.decorate(payload)
        print(f"Commissioning an Activity for: {builder}")
        await asyncio.sleep(random.randint(3, 7))
        return Activity(payload=payload)

    async def _run_batch(self, batch):
        results = []
        print(f"EXESCRIPT EXECUTION: {batch} on {batch.ctx.id}")
        for command in batch:
            print(f"EXESCRIPT COMMAND {command}")
            results.append({"command": command, "message": "some data here"})

        await asyncio.sleep(random.randint(1, 7))
        print(f"RETURNING RESULTS: {batch} on {batch.ctx.id}")
        return results

    async def _run_batches(self, batches: typing.AsyncGenerator):
        try:
            batch = await batches.__anext__()
            while batch:
                results = self._run_batch(batch)
                batch = await batches.asend(results)
        except StopAsyncIteration:
            print("RUN BATCHES - stop async iteration")
            pass

    def run_service(
        self,
        service_class: typing.Type[Service],
        num_instances: int = 1,
        payload: typing.Optional[Payload] = None,
    ) -> Cluster:
        payload = payload or service_class.get_payload()
        cluster = Cluster(executor=self, service_class=service_class, payload=payload)

        for i in range(num_instances):
            asyncio.create_task(self._run_batches(cluster.spawn_instance()))
        return cluster


###############################################################
#                                                             #
#                                                             #
#                          CLIENT CODE                        #
#                                                             #
#                                                             #
###############################################################


TURBOGETH_RUNTIME_NAME = "turbogeth-managed"
PROP_TURBOGETH_CHAIN = "golem.srv.app.eth.chain"


@dataclass
class TurbogethPayload(Payload):
    chain: str = prop(PROP_TURBOGETH_CHAIN)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=TURBOGETH_RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)


INSTANCES_NEEDED = 3
EXECUTOR_TIMEOUT = timedelta(weeks=100)


class TurbogethService(Service):
    credentials = None

    def post_init(self):
        self.credentials = {}

    def __repr__(self):
        srv_repr = super().__repr__()
        return f"{srv_repr}, credentials: {self.credentials}"

    @staticmethod
    def get_payload():
        return TurbogethPayload(chain="rinkeby")

    async def start(self):
        deploy_idx = self.ctx.deploy()
        self.ctx.start()
        future_results = yield self.ctx.commit()
        results = await future_results
        self.credentials = "RECEIVED" or results[deploy_idx]  # (NORMALLY, WOULD BE PARSED)

    async def run(self):

        while True:
            print(f"service {self.ctx.id} running on {self.ctx.provider_name} ... ")
            signal = self._listen_nowait()
            if signal and signal.message == "go":
                self.ctx.run("go!")
                yield self.ctx.commit()
            else:
                await asyncio.sleep(1)
                yield

    async def shutdown(self):
        self.ctx.download_file("some/service/state", "temp/path")
        yield self.ctx.commit()


async def main(subnet_tag, driver=None, network=None):

    async with Golem(
        max_workers=INSTANCES_NEEDED,
        budget=10.0,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
        event_consumer=log_summary(log_event_repr),
    ) as golem:
        cluster = golem.run_service(
            TurbogethService,
            # payload=payload,
            num_instances=INSTANCES_NEEDED,
        )

        def instances():
            return [{s.ctx.id, s.state.value} for s in cluster.instances]

        def still_running():
            return any([s for s in cluster.instances if s.is_available])

        cnt = 0
        while cnt < 10:
            print(f"instances: {instances()}")
            await asyncio.sleep(3)
            cnt += 1
            if cnt == 3:
                if len(cluster.instances) > 1:
                    cluster.instances[0].send_message_nowait("go")

        for s in cluster.instances:
            cluster.stop_instance(s)

        print(f"instances: {instances()}")

        cnt = 0
        while cnt < 10 and still_running():
            print(f"instances: {instances()}")
            await asyncio.sleep(1)

    print(f"instances: {instances()}")


asyncio.run(main(None))
