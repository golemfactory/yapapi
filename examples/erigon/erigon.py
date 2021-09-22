import asyncio

from dataclasses import dataclass

from yapapi.props.base import prop, constraint
from yapapi.props import inf

from yapapi.payload import Payload
from yapapi.executor import Golem
from yapapi.executor.services import Service
from yapapi.log import enable_default_logger


TURBOGETH_RUNTIME_NAME = "turbogeth-managed"
PROP_ERIGON_ETHEREUM_NETWORK = "golem.srv.app.eth.network"


@dataclass
class ErigonPayload(Payload):
    payment_network: str = prop(PROP_ERIGON_ETHEREUM_NETWORK)

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=TURBOGETH_RUNTIME_NAME)
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)


class ErigonService(Service):
    credentials = None

    def post_init(self):
        self.credentials = {}

    def __repr__(self):
        srv_repr = super().__repr__()
        return f"{srv_repr}, credentials: {self.credentials}"

    @staticmethod
    async def get_payload():
        return ErigonPayload(payment_network="rinkeby")

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


async def main(subnet_tag, payment_driver=None, payment_network=None):

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        cluster = await golem.run_service(
            ErigonService,
            num_instances=1,
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
