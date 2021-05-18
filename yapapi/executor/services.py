import asyncio
import typing
from dataclasses import dataclass, field
import enum
from typing import Any, List, Optional, Type
import statemachine

from yapapi.executor.ctx import WorkContext
from yapapi.payload import Payload

if typing.TYPE_CHECKING:
    from yapapi.executor import Golem


class ServiceState(statemachine.StateMachine):
    """
    State machine describing the state and lifecycle of a Service instance.
    """

    # states
    starting = statemachine.State("starting", initial=True)
    running = statemachine.State("running")
    stopping = statemachine.State("stopping")
    terminated = statemachine.State("terminated")
    unresponsive = statemachine.State("unresponsive")

    # transitions
    ready = starting.to(running)
    stop = running.to(stopping)
    terminate = terminated.from_(starting, running, stopping, terminated)
    mark_unresponsive = unresponsive.from_(starting, running, stopping, terminated)

    lifecycle = ready | stop | terminate

    # just a helper set of states in which the service can be interacted-with
    AVAILABLE = (starting, running, stopping)


@dataclass
class ServiceSignal:
    """
    Simple container to carry information between the client code and the Service instance.
    """

    message: Any
    response_to: Optional["ServiceSignal"] = None


class Service:
    """
    Base Service class to be extended by application developers to define their own,
    specialized Service specifications.
    """

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
    def get_payload() -> Optional[Payload]:
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
    """
    Control signal, used to request an instance's state change from the controlling Cluster.
    """
    stop = "stop"


@dataclass
class ServiceInstance:
    """Cluster's service instance.

    A binding between the instance of the Service, its control queue and its state,
    used by the Cluster to hold the complete state of each instance of a service.
    """

    service: Service
    control_queue: "asyncio.Queue[ControlSignal]" = field(default_factory=asyncio.Queue)
    service_state: ServiceState = field(default_factory=ServiceState)

    @property
    def state(self) -> ServiceState:
        return self.service_state.current_state

    def get_control_signal(self) -> Optional[ControlSignal]:
        try:
            return self.control_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    def send_control_signal(self, signal: ControlSignal):
        self.control_queue.put_nowait(signal)


class Cluster:
    """
    Golem's sub-engine used to spawn and control instances of a single Service.
    """

    def __init__(self, engine: "Golem", service_class: Type[Service], payload: Payload):
        self._engine = engine
        self.service_class = service_class

        self.payload = payload
        self.__instances: List[ServiceInstance] = []

    def __repr__(self):
        return f"Cluster " f"[Service: {self.service_class.__name__}, " f"Payload: {self.payload}]"

    @property
    def instances(self) -> List[Service]:
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

            # two potential issues:
            # * awaiting a batch makes us lose an ability to interpret a signal (await on generator won't return)
            # * we may be losing a `batch` when we act on the control signal
            #
            # potential solution:
            # * use `aiostream.stream.merge`

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
        act = await self._engine.get_activity(self.payload)
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
