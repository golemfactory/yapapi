"""Implementation of high-level services API."""
import asyncio
import inspect
import itertools
from dataclasses import dataclass, field
from datetime import timedelta, datetime, timezone
import enum
import logging
import statemachine  # type: ignore
import sys
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    Iterable,
    Dict,
)

if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack  # type: ignore

if sys.version_info >= (3, 8):
    from typing import Final
else:
    from typing_extensions import Final

from yapapi import rest, events
from yapapi.ctx import WorkContext
from yapapi.engine import _Engine, Job
from yapapi.network import Network, Node
from yapapi.payload import Payload
from yapapi.rest.activity import Activity, BatchError
from yapapi.script import Script


logger = logging.getLogger(__name__)

# current defaults for yagna providers as of yagna 0.6.x, see
# https://github.com/golemfactory/yagna/blob/c37dbd1a2bc918a511eed12f2399eb9fd5bbf2a2/agent/provider/src/market/negotiator/factory.rs#L20
MIN_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=5)
MAX_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=180)
DEFAULT_SERVICE_EXPIRATION: Final[timedelta] = MAX_AGREEMENT_EXPIRATION - timedelta(minutes=5)

cluster_ids = itertools.count(1)


class ServiceError(Exception):
    pass


class ServiceState(statemachine.StateMachine):
    """State machine describing the state and lifecycle of a :class:`Service` instance."""

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

    # transition performed when handler for the current state terminates normally,
    # that is, not due to an error or `ControlSignal.stop`
    lifecycle = ready | stop | terminate

    # transition performed on error or `ControlSignal.stop`
    error_or_stop = stop | terminate

    # just a helper set of states in which the service can be interacted-with
    AVAILABLE = (starting, running, stopping)

    instance: "ServiceInstance"

    def on_enter_state(self, state: statemachine.State):
        """Register `state` in the instance's list of visited states."""
        self.instance.visited_states.append(state)


@dataclass
class ServiceSignal:
    """Simple container to carry information between the client code and the Service instance."""

    message: Any
    response_to: Optional["ServiceSignal"] = None


# Return type for `sys.exc_info()`
ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]


class Service:
    """Base class for service specifications.

    To be extended by application developers to define their own, specialized
    Service specifications.
    """

    def __init__(self, cluster: "Cluster", ctx: WorkContext, network_node: Optional[Node] = None):
        """Initialize the service instance for a specific Cluster and a specific WorkContext.

        :param cluster: a cluster to which this service instance of this service belongs
        :param ctx: a work context object for executing commands on a provider that runs this
            service instance.
        """
        self._cluster: "Cluster" = cluster
        self._ctx: WorkContext = ctx
        self._network_node: Optional[Node] = network_node

        self.__inqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()
        self.__outqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()

        # Information on exception that caused the service change state,
        # as returned by `sys.exc_info()`.
        # Tuple of `None`'s means that the transition was not caused by an exception.
        self._exc_info: ExcInfo = (None, None, None)

        # TODO: maybe transition due to a control signal should also set this? To distinguish
        # stopping the service externally (e.g., via `cluster.stop()`) from internal transition
        # (e.g., via returning from `Service.run()`).

    @property
    def cluster(self) -> "Cluster":
        """Return the Cluster to which this service instance belongs."""
        return self._cluster

    @property
    def id(self) -> str:
        """Return the id of this service instance.

        Guaranteed to be unique within a :class:`~yapapi.services.Cluster`.
        """
        return self._ctx.id

    @property
    def provider_name(self) -> Optional[str]:
        """Return the name of the provider that runs this service instance."""
        return self._ctx.provider_name

    @property
    def provider_id(self) -> str:
        """Return the id of the provider that runs this service instance."""
        return self._ctx.provider_id

    @property
    def network(self) -> Optional[Network]:
        """Return the :class:`~yapapi.network.Network` to which this instance belongs (if any)"""
        return self.network_node.network if self.network_node else None

    @property
    def network_node(self) -> Optional[Node]:
        """Return the network :class:`~yapapi.network.Node` record associated with this instance."""
        return self._network_node

    def __repr__(self):
        return f"<{self.__class__.__name__} on {self.provider_name} [ {self.provider_id} ]>"

    def exc_info(self) -> ExcInfo:
        """Return exception info for an exception that caused the last state transition.

        If no such exception occurred, return `(None, None, None)`.
        """
        return self._exc_info

    async def send_message(self, message: Any = None):
        """Send a control message to this instance."""
        await self.__inqueue.put(ServiceSignal(message=message))

    def send_message_nowait(self, message: Optional[Any] = None):
        """Send a control message to this instance without blocking.

        May raise :class:`asyncio.QueueFull` if the channel for sending control messages is full.
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

        To be overridden by the author of a specific :class:`Service` class.

        If :func:`get_payload` is not implemented, the payload will need to be provided in the
        :func:`~yapapi.Golem.run_service` call.
        """
        pass

    def get_deploy_args(self) -> Dict:
        """Return the dictionary of kwargs needed to construct the `Deploy` exescript command."""
        kwargs = dict()
        if self._network_node:
            kwargs.update(self._network_node.get_deploy_args())
        return kwargs

    async def start(self) -> AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]]:
        """Implement the handler for the `starting` state of the service.

        To be overridden by the author of a specific :class:`Service` class.

        Should perform the minimum set of operations after which the instance of a service can be
        treated as "started", or, in other words, ready to receive service requests. It's up to the
        developer of the specific :class:`Service` class to decide what exact operations constitute a
        service startup. In the most common scenario :func:`~yapapi.script.Script.deploy()` and
        :func:`~yapapi.script.Script.start()` are required,
        check the `Default implementation` section for more details.

        As a handler implementing the `work generator pattern
        <https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals/hl-api-work-generator-pattern>`_,
        it's expected to be a generator that yields :class:`~yapapi.script.Script` (generated using the
        service's instance of the :class:`~yapapi.WorkContext` - :attr:`self._ctx`) that are then dispatched
        to the activity by the engine.

        Results of those batches can then be retrieved by awaiting the values captured from yield
        statements.

        A clean exit from a handler function triggers the engine to transition the state of the
        instance to the next stage in service's lifecycle - in this case, to `running`.

        On the other hand, any unhandled exception will cause the instance to be either retried on
        another provider node, if the :class:`Cluster`'s :attr:`respawn_unstarted_instances` argument is set to
        `True` in :func:`~yapapi.Golem.run_service`, which is also the default behavior, or altogether terminated, if
        :attr:`respawn_unstarted_instances` is set to `False`.

        **Example**::


            async def start(self):
                s = self._ctx.new_script()
                # deploy the exe-unit
                s.deploy(**self.get_deploy_args())
                # start the exe-unit's container
                s.start()
                # start some service process within the container
                s.run("/golem/run/service_ctl", "--start")
                # send the batch to the provider
                yield s

        ### Default implementation

        The default implementation assumes that, in order to accept commands, the runtime needs to
        be first deployed using the :func:`~yapapi.script.Script.deploy` command, which is analogous
        to creation of a container corresponding with the desired payload, and then started using the
        :func:`~yapapi.script.Script.start` command,
        actually launching the process that runs the aforementioned container.

        Additionally, it also assumes that the exe-unit doesn't need any additional parameters
        in its :func:`~yapapi.script.Script.start` call (e.g. for the VM runtime, all the required parameters are
        already passed as part of the agreement between the requestor and the provider), and parameters
        passed to :func:`~yapapi.script.Script.deploy` are returned by :func:`Service.get_deploy_args()` method.

        Therefore, this default implementation performs the minimum required for a VM payload to
        start responding to `run` commands. If your service requires any additional operations -
        you'll need to override this method (possibly first yielding from the parent - `super().start()` - generator)
        to add appropriate preparatory steps.

        In case of runtimes other than VM, `deploy` and/or `start` might be optional or altogether
        disallowed, or they may take some parameters. It is up to the author of the
        specific `Service` implementation that uses such a payload to adjust this method accordingly
        based on the requirements for the given runtime/exe-unit type.
        """

        s = self._ctx.new_script()
        s.deploy(**self.get_deploy_args())
        s.start()
        yield s

    async def run(self) -> AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]]:
        """Implement the handler for the `running` state of the service.

        To be overridden by the author of a specific :class:`Service` class.

        Should contain any operations needed to ensure continuous operation of a service.

        As a handler implementing the `work generator pattern
        <https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals/hl-api-work-generator-pattern>`_,
        it's expected to be a generator that yields :class:`~yapapi.script.Script` (generated using the
        service's instance of the :class:`~yapapi.WorkContext` - :attr:`self._ctx`) that are then dispatched
        to the activity by the engine.

        Results of those batches can then be retrieved by awaiting the values captured from `yield`
        statements.

        A clean exit from a handler function triggers the engine to transition the state of the
        instance to the next stage in service's lifecycle - in this case, to `stopping`.

        Any unhandled exception will cause the instance to be terminated.

        **Example**::

            async def run(self):
                while True:
                    script = self._ctx.new_script()
                    stats_results = script.run(self.SIMPLE_SERVICE, "--stats")
                    yield script
                    stats = (await stats_results).stdout.strip()
                    print(f"stats: {stats}")

        **Default implementation**

        Because the nature of the operations required during the "running" state depends directly
        on the specifics of a given :class:`Service` and because it's entirely plausible for a service
        not to require any direct interaction with the exe-unit (runtime) from the requestor's end
        after the service has been started, the default is to just wait indefinitely without
        producing any batches.
        """

        await asyncio.Future()
        yield  # type: ignore # unreachable because of the indefinite wait above

    async def shutdown(self) -> AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]]:
        """Implement the handler for the `stopping` state of the service.

        To be overridden by the author of a specific :class:`Service` class.

        Should contain any operations that the requestor needs to ensure the instance is correctly
        and gracefully shut-down - e.g. that its final state is retrieved.

        As a handler implementing the `work generator pattern
        <https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals/hl-api-work-generator-pattern>`_,
        it's expected to be a generator that yields :class:`~yapapi.script.Script` (generated using the
        service's instance of the :class:`~yapapi.WorkContext` - :attr:`self._ctx`) that are then dispatched
        to the activity by the engine.

        Results of those batches can then be retrieved by awaiting the values captured from yield
        statements.

        Finishing the execution of this handler will trigger termination of this instance.

        This handler will only be called if the activity running the service is still available.
        If the activity has already been deemed terminated or if the connection with the provider
        has been lost, the service will transition to the `terminated` state and the shutdown
        handler won't be run.

        **Example**::

            async def shutdown(self):
                self._ctx.run("/golem/run/dump_state")
                self._ctx.download_file("/golem/output/state", "/some/local/path/state")
                self._ctx.terminate()
                yield self._ctx.commit()

        **Default implementation**

        By default, the activity is just sent a `terminate` command. Whether it's absolutely
        required or not, again, depends on the implementation of the given runtime.

        """

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
    visited_states: List[statemachine.State] = field(default_factory=list)

    def __post_init__(self):
        self.service_state.instance = self

    @property
    def state(self) -> ServiceState:
        """Return the current state of this instance."""
        return self.service_state.current_state

    @property
    def started_successfully(self) -> bool:
        """Return `True` if this instance has entered `running` state, `False` otherwise."""
        return ServiceState.running in self.visited_states


class ClusterState(statemachine.StateMachine):
    running = statemachine.State("running", initial=True)
    stopping = statemachine.State("stopping")

    stop = running.to(stopping)


class Cluster(AsyncContextManager):
    """Golem's sub-engine used to spawn and control instances of a single :class:`Service`."""

    def __init__(
        self,
        engine: "_Engine",
        service_class: Type[Service],
        payload: Payload,
        expiration: Optional[datetime] = None,
        respawn_unstarted_instances: bool = True,
        network: Optional[Network] = None,
    ):
        """Initialize this Cluster.

        :param engine: an engine for running service instance
        :param service_class: service specification
        :param payload: definition of service runtime for this Cluster
        :param expiration: a date before which all agreements related to running services
            in this Cluster should be terminated
        :param respawn_unstarted_instances: if an instance fails in the `starting` state,
            should this Cluster try to spawn another instance
        :param network: optional Network representing the VPN that this Cluster's instances will
            be attached to.
        """

        self.id = str(next(cluster_ids))

        self._engine = engine
        self._service_class = service_class
        self._payload = payload
        self._expiration: datetime = (
            expiration or datetime.now(timezone.utc) + DEFAULT_SERVICE_EXPIRATION
        )
        self._task_ids = itertools.count(1)
        self._stack = AsyncExitStack()
        self._respawn_unstarted_instances = respawn_unstarted_instances

        self.__instances: List[ServiceInstance] = []
        """List of Service instances"""

        self._instance_tasks: Set[asyncio.Task] = set()
        """Set of asyncio tasks that run spawn_service()"""

        self._network: Optional[Network] = network

        self.__state = ClusterState()

    @property
    def expiration(self) -> datetime:
        """Return the expiration datetime for agreements related to services in this :class:`Cluster`."""
        return self._expiration

    @property
    def payload(self) -> Payload:
        """Return the service runtime definition for this :class:`Cluster`."""
        return self._payload

    @property
    def service_class(self) -> Type[Service]:
        """Return the class instantiated by all service instances in this :class:`Cluster`."""
        return self._service_class

    @property
    def network(self) -> Optional[Network]:
        """Return the :class:`~yapapi.network.Network` record associated with the VPN used by this :class:`Cluster`."""
        return self._network

    def __repr__(self):
        return (
            f"Cluster {self.id}: {len(self.__instances)}x[Service: {self._service_class.__name__}, "
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
        """Emit an event using this :class:`Cluster`'s engine."""
        self._engine.emit(event)

    @property
    def instances(self) -> List[Service]:
        """Return the list of service instances in this :class:`Cluster`."""
        return [i.service for i in self.__instances]

    def __get_service_instance(self, service: Service) -> ServiceInstance:
        for i in self.__instances:
            if i.service == service:
                return i
        assert False, f"No instance found for {service}"

    def get_state(self, service: Service) -> ServiceState:
        """Return the state of the specific instance in this :class:`Cluster`."""
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
            if inspect.isasyncgenfunction(handler):
                return handler()
            else:
                service_cls_name = type(instance.service).__name__
                handler_name = handler.__name__
                raise TypeError(
                    f"Service handler: `{service_cls_name}.{handler_name}` must be an asynchronous generator."
                )

    @staticmethod
    def _change_state(
        instance: ServiceInstance,
        event: Union[ControlSignal, ExcInfo] = (None, None, None),
    ) -> bool:
        """Initiate a state transition for `instance` caused by `event`.

        Return `True` if instance's state changed as result of the transition
        (i.e., if resulting state is different from the original one),
        `False` otherwise.

        The transition may be due to a control signal, an error,
        or the handler method for the current state terminating normally.
        """
        prev_state = instance.state

        if event == (None, None, None):
            # Normal, i.e. non-error, state transition
            instance.service_state.lifecycle()
        elif isinstance(event, tuple) or event == ControlSignal.stop:
            # Transition on error or `stop` signal
            instance.service_state.error_or_stop()
        else:
            # Unhandled signal, don't change the state
            assert isinstance(event, ControlSignal)
            logger.warning("Don't know how to handle control signal %s", event)

        if isinstance(event, tuple):
            instance.service._exc_info = event
        return instance.state != prev_state

    async def _run_instance(self, instance: ServiceInstance):
        loop = asyncio.get_event_loop()
        self.__instances.append(instance)

        logger.info("%s commissioned", instance.service)

        handler = None
        batch_task: Optional[asyncio.Task] = None
        signal_task: Optional[asyncio.Task] = None

        def update_handler(instance: ServiceInstance):
            nonlocal handler

            try:
                # if handler cannot be obtained, it's not changed here, but in change_state
                handler = self._get_handler(instance)
                logger.debug("%s state changed to %s", instance.service, instance.state.value)
            except Exception:
                logger.error(
                    "Error getting '%s' handler for %s: %s",
                    instance.state.value,
                    instance.service,
                    sys.exc_info(),
                )
                change_state(sys.exc_info())

        def change_state(event: Union[ControlSignal, ExcInfo] = (None, None, None)) -> None:
            """Initiate state transition, due to a signal, an error, or handler termination."""
            nonlocal batch_task, handler, instance

            if self._change_state(instance, event):
                update_handler(instance)

            if batch_task:
                batch_task.cancel()
                # TODO: await batch_task here?
            batch_task = None

        update_handler(instance)

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
                    # Errors in service code will be raised by `batch_task.result()`.
                    # These include:
                    # - StopAsyncIteration's resulting from normal exit from handler function
                    # - BatchErrors that were thrown into the service code by
                    #   the `except BatchError` clause below
                    batch = batch_task.result()
                except StopAsyncIteration:
                    change_state()

                    # work-around an issue preventing nodes from getting correct information about
                    # each other on instance startup by re-sending the information after the service
                    # transitions to the running state
                    if self.network and instance.state == ServiceState.running:
                        await self.network.refresh_nodes()

                except Exception:
                    logger.warning("Unhandled exception in service", exc_info=True)
                    change_state(sys.exc_info())
                else:
                    try:
                        # Errors in commands executed on provider will be raised here:
                        fut_result = yield batch
                    except BatchError:
                        # Throw the error into the service code so it can be handled there
                        logger.debug("Batch execution failed", exc_info=True)
                        batch_task = loop.create_task(handler.athrow(*sys.exc_info()))
                    except Exception:
                        # Could be an ApiException thrown in call_exec or get_exec_batch_results
                        # operations of Activity API. Currently we do not pass such exceptions
                        # to service handlers but initiate a state transition
                        logger.error("Unhandled engine error", exc_info=True)
                        change_state(sys.exc_info())
                    else:
                        batch_task = loop.create_task(handler.asend(fut_result))

            if signal_task in done:
                # Process a signal
                ctl = signal_task.result()
                logger.debug("Processing control signal %s", ctl)
                change_state(ctl)
                signal_task = None

        logger.debug("No handler for %s in state %s", instance.service, instance.state.value)

        try:
            if batch_task:
                batch_task.cancel()
                await batch_task
            if signal_task:
                signal_task.cancel()
                await signal_task
        except asyncio.CancelledError:
            pass

        logger.info("%s decommissioned", instance.service)

    async def spawn_instance(self, params: Dict, network_address: Optional[str] = None) -> None:
        """Spawn a new service instance within this :class:`Cluster`."""

        logger.debug("spawning instance within %s", self)
        instance: Optional[ServiceInstance] = None
        agreement_id: Optional[str]  # set in start_worker

        async def _worker(
            agreement: rest.market.Agreement, activity: Activity, work_context: WorkContext
        ) -> None:
            nonlocal agreement_id, instance
            agreement_id = agreement.id

            task_id = f"{self.id}:{next(self._task_ids)}"
            self.emit(
                events.TaskStarted(
                    job_id=self._job.id,
                    agr_id=agreement.id,
                    task_id=task_id,
                    task_data=f"Service: {self._service_class.__name__}",
                )
            )
            # prepare the Node entry for this instance, if the cluster is attached to a VPN
            node: Optional[Node] = None
            if self.network:
                node = await self.network.add_node(work_context.provider_id, network_address)

            instance = ServiceInstance(
                service=self._service_class(self, work_context, network_node=node, **params)  # type: ignore
            )
            try:
                if self._state == ClusterState.running:
                    instance_batches = self._run_instance(instance)
                    try:
                        await self._engine.process_batches(
                            self._job.id, agreement.id, activity, instance_batches
                        )
                    except StopAsyncIteration:
                        pass

                self.emit(
                    events.TaskFinished(
                        job_id=self._job.id,
                        agr_id=agreement.id,
                        task_id=task_id,
                    )
                )
                self.emit(events.WorkerFinished(job_id=self._job.id, agr_id=agreement.id))
            finally:
                await self._engine.accept_payments_for_agreement(self._job.id, agreement.id)
                await self._job.agreements_pool.release_agreement(agreement.id, allow_reuse=False)

        while instance is None and self._state == ClusterState.running:
            agreement_id = None
            await asyncio.sleep(1.0)
            task = await self._engine.start_worker(self._job, _worker)
            if not task:
                continue
            try:
                await task
                if (
                    # if the instance was created ...
                    instance
                    # but failed to start ...
                    and not instance.started_successfully
                    # due to an error (and not a `STOP` signal) ...
                    and instance.service.exc_info() != (None, None, None)
                    # and re-spawning instances is enabled for this cluster
                    and self._respawn_unstarted_instances
                ):
                    logger.warning("Instance failed when starting, trying to create another one...")
                    instance = None
            except Exception:
                if agreement_id:
                    self.emit(
                        events.WorkerFinished(
                            job_id=self._job.id, agr_id=agreement_id, exc_info=sys.exc_info()
                        )
                    )
                else:
                    # This shouldn't happen, we may log and return as well
                    logger.error("Failed to spawn instance", exc_info=True)
                    return

    def stop_instance(self, service: Service):
        """Stop the specific :class:`Service` instance belonging to this :class:`Cluster`."""

        instance = self.__get_service_instance(service)
        instance.control_queue.put_nowait(ControlSignal.stop)

    def spawn_instances(
        self,
        num_instances: Optional[int] = None,
        instance_params: Optional[Iterable[Dict]] = None,
        network_addresses: Optional[List[str]] = None,
    ) -> None:
        """Spawn new instances within this :class:`Cluster`.

        :param num_instances: optional number of service instances to run. Defaults to a single
            instance, unless `instance_params` is given, in which case, the :class:`Cluster` will spawn
            as many instances as there are elements in the `instance_params` iterable.
            if `num_instances` is not None and < 1, the method will immediately return and log a warning.
        :param instance_params: optional list of dictionaries of keyword arguments that will be passed
            to consecutive, spawned instances. The number of elements in the iterable determines the
            number of instances spawned, unless `num_instances` is given, in which case the latter takes
            precedence.
            In other words, if both `num_instances` and `instance_params` are provided,
            the number of instances spawned will be equal to `num_instances` and if there are
            too few elements in the `instance_params` iterable, it will results in an error.
        :param network_addresses: optional list of network addresses in case the :class:`Cluster` is
            attached to VPN. If the list is not provided (or if the number of elements is less
            than the number of spawned instances), any instances for which the addresses have not
            been given, will be assigned an address automatically.

        """
        # just a sanity check
        if num_instances is not None and num_instances < 1:
            logger.warning(
                "Trying to spawn less than one instance. num_instances: %s", num_instances
            )
            return

        # if the parameters iterable was not given, assume a default of a single instance
        if not num_instances and not instance_params:
            num_instances = 1

        # convert the parameters iterable to an iterator
        # if not provided, make a default iterator consisting of empty dictionaries
        instance_params = iter(instance_params or (dict() for _ in range(num_instances)))  # type: ignore

        # supply network_addresses as long as there are any still left
        if network_addresses is None:
            network_addresses = []
        network_addresses_generator = (
            network_addresses[i] if i < len(network_addresses) else None for i in itertools.count()
        )

        loop = asyncio.get_event_loop()
        spawned_instances = 0
        while not num_instances or spawned_instances < num_instances:
            try:
                params = next(instance_params)
                network_address = next(network_addresses_generator)
                task = loop.create_task(self.spawn_instance(params, network_address))
                self._instance_tasks.add(task)
                spawned_instances += 1
            except StopIteration:
                if num_instances and spawned_instances < num_instances:
                    raise ServiceError(
                        f"`instance_params` iterable depleted after {spawned_instances} spawned instances."
                    )
                break

    def stop(self):
        """Signal the whole :class:`Cluster` to stop."""
        self.__state.stop()

        for s in self.instances:
            self.stop_instance(s)

    @property
    def _state(self):
        """Current state of the Cluster."""
        return self.__state.current_state
