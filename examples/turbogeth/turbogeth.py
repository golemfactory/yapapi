import asyncio
import enum
from datetime import timedelta
import typing
from typing import Optional, Any
import random

import statemachine


from dataclasses import dataclass, field
# from yapapi.executor.ctx import WorkContext
from yapapi.props.builder import DemandBuilder, AutodecoratingModel
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

_act_cnt = 0

def _act_id():
    global _act_cnt
    _act_cnt += 1
    return _act_cnt

@dataclass
class Activity:
    """ Mock Activity """
    payload: Payload
    id: int = field(default_factory=_act_id)


class Steps(list):
    """ Mock Steps to illustrate the idea. """
    def __init__(self, *args, **kwargs):
        self.ctx: WorkContext = kwargs.pop('ctx')
        self.blocking: bool = kwargs.pop('blocking', True)

        super().__init__(*args, **kwargs)

    def __repr__(self):
        list_repr = super().__repr__()
        return f"<{self.__class__.__name__}: {list_repr}, blocking: {self.blocking}"

class WorkContext:
    """ Mock WorkContext to illustrate the idea. """

    def __init__(self, activity_id, provider_name):
        self.id = activity_id
        self.provider_name = provider_name
        self._steps = Steps(ctx=self)

    def __repr__(self):
        return f"<{self.__class__.__name__}: activity_id: {self.id}>"

    def __str__(self):
        return self.__repr__()

    def commit(self, blocking: bool = True):
        self._steps.blocking = blocking
        steps = self._steps
        self._steps = Steps(ctx=self)
        return steps

    def __getattr__(self, item):
        def add_step(*args, **kwargs) -> int:
            idx = len(self._steps)
            self._steps.append({item: (args, kwargs)})
            return idx

        return add_step


@dataclass
class ServiceSignal:
    """ THIS WOULD BE PART OF THE API CODE"""
    message: Any
    response_to: Optional["ServiceSignal"] = None


class ConfigurationError(Exception):
    """ THIS WOULD BE PART OF THE API CODE"""
    pass


class ServiceState(statemachine.StateMachine):
    """ THIS WOULD BE PART OF THE API CODE"""
    starting = statemachine.State("starting", initial=True)
    running = statemachine.State("running")
    stopping = statemachine.State("stopping")
    terminated = statemachine.State("terminated")
    unresponsive = statemachine.State("unresponsive")

    ready = starting.to(running)
    stop = running.to(stopping)
    terminate = terminated.from_(starting, running, stopping, terminated)
    mark_unresponsive = unresponsive.from_(starting, running, stopping, terminated)

    lifecycle = ready | stop | terminate

    AVAILABLE = (
        starting, running, stopping
    )


class Service:
    """ THIS WOULD BE PART OF THE API CODE"""

    def __init__(self, cluster: "Cluster", ctx: WorkContext):
        self.cluster = cluster
        self.ctx = ctx

        self.__inqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()
        self.__outqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()
        self.post_init()

    def post_init(self):
        pass

    @property
    def id(self):
        return self.ctx.id

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.id}>"

    async def send_message(self, message: Any = None):
        await self.__inqueue.put(ServiceSignal(message=message))

    def send_message_nowait(self, message: Optional[Any] = None):
        self.__inqueue.put_nowait(ServiceSignal(message=message))

    async def receive_message(self) -> ServiceSignal:
        return await self.__outqueue.get()

    def receive_message_nowait(self) -> Optional[ServiceSignal]:
        try:
            return self.__outqueue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    async def _listen(self) -> ServiceSignal:
        return await self.__inqueue.get()

    def _listen_nowait(self) -> Optional[ServiceSignal]:
        try:
            return self.__inqueue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    async def _respond(self, message: Optional[Any], response_to: Optional[ServiceSignal] = None):
        await self.__outqueue.put(ServiceSignal(message=message, response_to=response_to))

    def _respond_nowait(self, message: Optional[Any], response_to: Optional[ServiceSignal] = None):
        self.__outqueue.put_nowait(ServiceSignal(message=message, response_to=response_to))

    @staticmethod
    def get_payload() -> typing.Optional[Payload]:
        """Return the payload (runtime) definition for this service.

        If `get_payload` is not implemented, the payload will need to be provided in the
        `Golem.run_service` call.
        """
        pass

    async def start(self):
        self.ctx.deploy()
        self.ctx.start()
        yield self.ctx.commit()

    async def run(self):
        while True:
            await asyncio.sleep(10)
            yield

    async def shutdown(self):
        yield

    @property
    def is_available(self):
        return self.cluster.get_state(self) in ServiceState.AVAILABLE

    @property
    def state(self):
        return self.cluster.get_state(self)


class ControlSignal(enum.Enum):
    stop = "stop"

@dataclass
class ServiceInstance:
    """ THIS WOULD BE PART OF THE API CODE"""
    service: Service
    control_queue: "asyncio.Queue[ControlSignal]" = field(default_factory=asyncio.Queue)
    service_state: ServiceState = field(default_factory=ServiceState)

    @property
    def state(self) -> ServiceState:
        return self.service_state.current_state

    def get_control_signal(self) -> typing.Optional[ControlSignal]:
        try:
            return self.control_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    def send_control_signal(self, signal: ControlSignal):
        self.control_queue.put_nowait(signal)


class Cluster:
    """ THIS WOULD BE PART OF THE API CODE"""

    def __init__(self, executor: "Golem", service_class: typing.Type[Service], payload: Payload):
        self.executor = executor
        self.service_class = service_class

        if not payload:
            raise ConfigurationError("Payload must be defined when starting a cluster.")

        self.payload = payload
        self.__instances: typing.List[ServiceInstance] = []

    def __repr__(self):
        return f"Cluster " \
               f"[Service: {self.service_class.__name__}, " \
               f"Payload: {self.payload}]"

    @property
    def instances(self) -> typing.List[Service]:
        return [i.service for i in self.__instances]

    def __get_service_instance(self, service: Service) -> ServiceInstance:
        for i in self.__instances:
            if i.service == service:
                return i

    def get_state(self, service: Service):
        instance = self.__get_service_instance(service)
        return instance.state

    @staticmethod
    def _get_handler(instance: ServiceInstance):
        _handlers = {
            ServiceState.starting: instance.service.start,
            ServiceState.running: instance.service.run,
            ServiceState.stopping: instance.service.shutdown,
        }
        handler = _handlers.get(instance.state, None)
        if handler:
            return handler()

    async def _run_instance(self, ctx: WorkContext):
        loop = asyncio.get_event_loop()
        instance = ServiceInstance(service=self.service_class(self, ctx))
        self.__instances.append(instance)

        print(f"{instance.service} commissioned")

        handler = self._get_handler(instance)
        batch = None

        while handler:
            try:
                if batch:
                    r = yield batch
                    fr = loop.create_future()
                    fr.set_result(await r)
                    batch = await handler.asend(fr)
                else:
                    batch = await handler.__anext__()
            except StopAsyncIteration:
                instance.service_state.lifecycle()
                handler = self._get_handler(instance)
                batch = None
                print(f"{instance.service} state changed to {instance.state.value}")

            ctl = instance.get_control_signal()
            if ctl == ControlSignal.stop:
                if instance.state == ServiceState.running:
                    instance.service_state.stop()
                else:
                    instance.service_state.terminate()

                print(f"{instance.service} state changed to {instance.state.value}")

                handler = self._get_handler(instance)
                batch = None

        print(f"{instance.service} decomissioned")

    async def spawn_instance(self):
        act = await self.executor.get_activity(self.payload)
        ctx = WorkContext(act.id, len(self.instances) + 1)

        instance_batches = self._run_instance(ctx)
        try:
            batch = await instance_batches.__anext__()
            while batch:
                r = yield batch
                batch = await instance_batches.asend(r)
        except StopAsyncIteration:
            pass

    def stop_instance(self, service: Service):
        instance = self.__get_service_instance(service)
        instance.send_control_signal(ControlSignal.stop)


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
        await asyncio.sleep(random.randint(3, 7))
        return Activity(payload=payload)

    async def _run_batch(self, batch):
        results = []
        print(f"EXESCRIPT EXECUTION: {batch} on {batch.ctx.id}")
        for command in batch:
            print(f"EXESCRIPT COMMAND {command}")
            results.append({
                'command': command,
                'message': 'some data here'
            })

        await asyncio.sleep(random.randint(1,7))
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
PROP_TURBOGETH_RPC_PORT = "golem.srv.app.eth.rpc-port"


@dataclass
class TurbogethPayload(Payload, AutodecoratingModel):
    rpc_port: int = prop(PROP_TURBOGETH_RPC_PORT, default=None)

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
        return TurbogethPayload(rpc_port=8888)

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
            num_instances=INSTANCES_NEEDED
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

