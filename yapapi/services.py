"""Implementation of high-level services API."""
import asyncio
import itertools
from dataclasses import dataclass, field
from datetime import timedelta, datetime, timezone
import enum
import logging
from typing import Any, AsyncContextManager, List, Optional, Set, Type
import statemachine  # type: ignore
import sys

if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack  # type: ignore

if sys.version_info >= (3, 8):
    from typing import Final
else:
    from typing_extensions import Final

from yapapi import Golem, Job, rest
from yapapi.executor import events
from yapapi.executor.ctx import WorkContext
from yapapi.payload import Payload
from yapapi.props import NodeInfo


logger = logging.getLogger(__name__)

# current defaults for yagna providers as of yagna 0.6.x, see
# https://github.com/golemfactory/yagna/blob/c37dbd1a2bc918a511eed12f2399eb9fd5bbf2a2/agent/provider/src/market/negotiator/factory.rs#L20
MIN_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=5)
MAX_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=180)
DEFAULT_SERVICE_EXPIRATION: Final[timedelta] = MAX_AGREEMENT_EXPIRATION - timedelta(minutes=5)

cluster_ids = itertools.count(1)


class ServiceState(statemachine.StateMachine):
    """State machine describing the state and lifecycle of a Service instance."""

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
    """Simple container to carry information between the client code and the Service instance."""

    message: Any
    response_to: Optional["ServiceSignal"] = None


class Service:
    """Base class for service specifications.

    To be extended by application developers to define their own, specialized
    Service specifications.
    """

    def __init__(self, cluster: "Cluster", ctx: WorkContext):
        """Initialize the service instance for a specific Cluster and a specific WorkContext.

        :param cluster: a cluster to which this service instance of this service belongs
        :param ctx: a work context object for executing commands on a provider that runs this
            service instance.
        """
        self._cluster: "Cluster" = cluster
        self._ctx: WorkContext = ctx

        self.__inqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()
        self.__outqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()

    @property
    def id(self):
        """Return the id of this service instance.

        Guaranteed to be unique within a Cluster.
        """
        return self._ctx.id

    @property
    def provider_name(self):
        """Return the name of the provider that runs this service instance."""
        return self._ctx.provider_name

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.id}>"

    async def send_message(self, message: Any = None):
        """Send a control message to this instance."""
        await self.__inqueue.put(ServiceSignal(message=message))

    def send_message_nowait(self, message: Optional[Any] = None):
        """Send a control message to this instance without blocking.

        May raise `asyncio.QueueFull` if the channel for sending control messages is full.
        """
        self.__inqueue.put_nowait(ServiceSignal(message=message))

    async def receive_message(self) -> ServiceSignal:
        """Wait for a control message sent to this instance."""
        return await self.__outqueue.get()

    def receive_message_nowait(self) -> Optional[ServiceSignal]:
        """Retrieve a control message sent to this instance.

        Return `None` if no message is available.
        """
        try:
            return self.__outqueue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _listen(self) -> ServiceSignal:
        return await self.__inqueue.get()

    def _listen_nowait(self) -> Optional[ServiceSignal]:
        try:
            return self.__inqueue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _respond(self, message: Optional[Any], response_to: Optional[ServiceSignal] = None):
        await self.__outqueue.put(ServiceSignal(message=message, response_to=response_to))

    def _respond_nowait(self, message: Optional[Any], response_to: Optional[ServiceSignal] = None):
        self.__outqueue.put_nowait(ServiceSignal(message=message, response_to=response_to))

    @staticmethod
    async def get_payload() -> Optional[Payload]:
        """Return the payload (runtime) definition for this service.

        If `get_payload` is not implemented, the payload will need to be provided in the
        `Golem.run_service` call.
        """
        pass

    async def start(self):
        """Implement the `starting` state of the service."""

        self._ctx.deploy()
        self._ctx.start()
        yield self._ctx.commit()

    async def run(self):
        """Implement the `running` state of the service."""

        while True:
            await asyncio.sleep(10)
            yield

    async def shutdown(self):
        """Implement the `stopping` state of the service."""

        self._ctx.terminate()
        yield self._ctx.commit()

    @property
    def is_available(self):
        """Return `True` iff this instance is available (that is, starting, running or stopping)."""
        return self._cluster.get_state(self) in ServiceState.AVAILABLE

    @property
    def state(self):
        """Return the current state of this instance."""
        return self._cluster.get_state(self)


class ControlSignal(enum.Enum):
    """Control signal, used to request an instance's state change from the controlling Cluster."""

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
        """Return the current state of this instance."""
        return self.service_state.current_state


class Cluster(AsyncContextManager):
    """Golem's sub-engine used to spawn and control instances of a single Service."""

    def __init__(
        self,
        engine: "Golem",
        service_class: Type[Service],
        payload: Payload,
        num_instances: int = 1,
        expiration: Optional[datetime] = None,
    ):
        """Initialize this Cluster.

        :param engine: an engine for running service instance
        :param service_class: service specification
        :param payload: definition of service runtime for this Cluster
        :param num_instances: number of instances to spawn in this Cluster
        :param expiration: a date before which all agreements related to running services
            in this Cluster should be terminated
        """

        self.id = str(next(cluster_ids))

        self._engine = engine
        self._service_class = service_class
        self._payload = payload
        self._num_instances = num_instances
        self._expiration = expiration or datetime.now(timezone.utc) + DEFAULT_SERVICE_EXPIRATION
        self._task_ids = itertools.count(1)
        self._stack = AsyncExitStack()

        self.__instances: List[ServiceInstance] = []
        """List of Service instances"""

        self._instance_tasks: Set[asyncio.Task] = set()
        """Set of asyncio tasks that run spawn_service()"""

    def __repr__(self):
        return (
            f"Cluster {self.id}: {self._num_instances}x[Service: {self._service_class.__name__}, "
            f"Payload: {self._payload}]"
        )

    async def __aenter__(self):
        """Post a Demand and start collecting provider Offers for running service instances."""

        self.__services: Set[asyncio.Task] = set()
        """Asyncio tasks running within this cluster"""

        logger.debug("Starting new %s", self)

        self._job = Job(self._engine, expiration_time=self._expiration, payload=self._payload)
        self._engine.add_job(self._job)

        loop = asyncio.get_event_loop()
        self.__services.add(loop.create_task(self._job.find_offers()))

        async def agreements_pool_cycler():
            # shouldn't this be part of the Agreement pool itself? (or a task within Job?)
            while True:
                await asyncio.sleep(2)
                await self._job.agreements_pool.cycle()

        self.__services.add(loop.create_task(agreements_pool_cycler()))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release resources used by this Cluster."""

        logger.debug("%s is shutting down...", self)

        # Give the instance tasks some time to terminate gracefully.
        # Then cancel them without mercy!
        if self._instance_tasks:
            logger.debug("Waiting for service instances to terminate...")
            _, still_running = await asyncio.wait(self._instance_tasks, timeout=10)
            if still_running:
                for task in still_running:
                    logger.debug("Cancelling task: %s", task)
                    task.cancel()
                await asyncio.gather(*still_running, return_exceptions=True)

        # TODO: should be different if we stop due to an error
        termination_reason = {
            "message": "Successfully finished all work",
            "golem.requestor.code": "Success",
        }

        try:
            logger.debug("Terminating agreements...")
            await self._job.agreements_pool.terminate_all(reason=termination_reason)
        except Exception:
            logger.debug("Couldn't terminate agreements", exc_info=True)

        for task in self.__services:
            if not task.done():
                logger.debug("Cancelling task: %s", task)
                task.cancel()
        await asyncio.gather(*self.__services, return_exceptions=True)

        self._engine.finalize_job(self._job)

    def emit(self, event: events.Event) -> None:
        """Emit an event using this Cluster's engine."""
        self._engine.emit(event)

    @property
    def instances(self) -> List[Service]:
        """Return the list of service instances in this Cluster."""
        return [i.service for i in self.__instances]

    def __get_service_instance(self, service: Service) -> ServiceInstance:
        for i in self.__instances:
            if i.service == service:
                return i
        assert False, f"No instance found for {service}"

    def get_state(self, service: Service) -> ServiceState:
        """Return the state of the specific instance in this Cluster."""
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
        instance = ServiceInstance(service=self._service_class(self, ctx))
        self.__instances.append(instance)

        logger.info("%s commissioned", instance.service)

        handler = self._get_handler(instance)

        batch_task: Optional[asyncio.Task] = None
        signal_task: Optional[asyncio.Task] = None

        while handler:
            # Repeatedly wait on one of `(batch_task, signal_task)` to finish.
            # If it's the first one, retrieve a batch from its result and handle it.
            # If it's the second -- retrieve and handle a signal.
            # Any finished task is replaced with a new one, so there are always two.

            if batch_task is None:
                batch_task = loop.create_task(handler.__anext__())
            if signal_task is None:
                signal_task = loop.create_task(instance.control_queue.get())

            done, _ = await asyncio.wait(
                (batch_task, signal_task), return_when=asyncio.FIRST_COMPLETED
            )

            if batch_task in done:
                # Process a batch
                try:
                    batch = batch_task.result()
                    fut_result = yield batch
                    result = await fut_result
                    wrapped_results = loop.create_future()
                    wrapped_results.set_result(result)
                    batch_task = loop.create_task(handler.asend(wrapped_results))
                except StopAsyncIteration:
                    instance.service_state.lifecycle()
                    handler = None
                    batch_task = None

            if signal_task in done:
                # Process a signal
                ctl = signal_task.result()
                logger.debug("Processing control signal %s", ctl)
                if ctl == ControlSignal.stop:
                    if instance.state == ServiceState.running:
                        instance.service_state.stop()
                    else:
                        instance.service_state.terminate()
                    handler = None
                    if batch_task:
                        batch_task.cancel()
                        batch_task = None
                signal_task = None

            if handler is None:
                handler = self._get_handler(instance)
                logger.debug("%s state changed to %s", instance.service, instance.state.value)

        logger.debug("No handler for %s in state %s", instance.service, instance.state.value)

        if batch_task:
            batch_task.cancel()
            await batch_task
        if signal_task:
            signal_task.cancel()
            await signal_task

        logger.info("%s decomissioned", instance.service)

    async def spawn_instance(self) -> None:
        """Spawn a new service instance within this Cluster."""

        logger.debug("spawning instance within %s", self)
        spawned = False

        async def start_worker(agreement: rest.market.Agreement, node_info: NodeInfo) -> None:
            nonlocal spawned
            self.emit(events.WorkerStarted(agr_id=agreement.id))
            try:
                act = await self._engine.create_activity(agreement.id)
            except Exception:
                self.emit(
                    events.ActivityCreateFailed(
                        agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                    )
                )
                self.emit(events.WorkerFinished(agr_id=agreement.id))
                raise

            async with act:
                spawned = True
                self.emit(events.ActivityCreated(act_id=act.id, agr_id=agreement.id))
                self._engine.accept_debit_notes_for_agreement(agreement.id)
                work_context = WorkContext(
                    act.id, node_info, self._engine.storage_manager, emitter=self.emit
                )
                task_id = f"{self.id}:{next(self._task_ids)}"
                self.emit(
                    events.TaskStarted(
                        agr_id=agreement.id,
                        task_id=task_id,
                        task_data=f"Service: {self._service_class.__name__}",
                    )
                )

                try:
                    instance_batches = self._run_instance(work_context)
                    try:
                        await self._engine.process_batches(agreement.id, act, instance_batches)
                    except StopAsyncIteration:
                        pass

                    self.emit(
                        events.TaskFinished(
                            agr_id=agreement.id,
                            task_id=task_id,
                        )
                    )
                    self.emit(events.WorkerFinished(agr_id=agreement.id))
                except Exception:
                    self.emit(
                        events.WorkerFinished(
                            agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                        )
                    )
                    raise
                finally:
                    await self._engine.accept_payments_for_agreement(agreement.id)
                    await self._job.agreements_pool.release_agreement(
                        agreement.id, allow_reuse=False
                    )

        loop = asyncio.get_event_loop()

        while not spawned:
            await asyncio.sleep(1.0)
            task = await self._job.agreements_pool.use_agreement(
                lambda agreement, node: loop.create_task(start_worker(agreement, node))
            )
            if task:
                await task

    def stop_instance(self, service: Service):
        """Stop the specific service instance belonging to this Cluster."""

        instance = self.__get_service_instance(service)
        instance.control_queue.put_nowait(ControlSignal.stop)

    def spawn_instances(self, num_instances: Optional[int] = None) -> None:
        """Spawn new instances within this Cluster.

        :param num_instances: number of instances to commission.
            if not given, spawns the number that the Cluster has been initialized with.
        """
        if num_instances:
            self._num_instances += num_instances
        else:
            num_instances = self._num_instances

        loop = asyncio.get_event_loop()
        for i in range(num_instances):
            task = loop.create_task(self.spawn_instance())
            self._instance_tasks.add(task)

    def stop(self):
        """Signal the whole cluster to stop."""
        for s in self.instances:
            self.stop_instance(s)
