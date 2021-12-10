"""Implementation of high-level services API."""
import asyncio
import inspect
import itertools
from datetime import timedelta, datetime, timezone
import enum
import logging
import statemachine  # type: ignore
import sys
from types import TracebackType
from typing import (
    AsyncContextManager,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
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
from yapapi.network import Network
from yapapi.payload import Payload
from yapapi.rest.activity import Activity, BatchError

from .service import Service, ServiceInstance
from .service_state import ServiceState

logger = logging.getLogger(__name__)

# current defaults for yagna providers as of yagna 0.6.x, see
# https://github.com/golemfactory/yagna/blob/c37dbd1a2bc918a511eed12f2399eb9fd5bbf2a2/agent/provider/src/market/negotiator/factory.rs#L20
MIN_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=5)
MAX_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=180)
DEFAULT_SERVICE_EXPIRATION: Final[timedelta] = MAX_AGREEMENT_EXPIRATION - timedelta(minutes=5)

cluster_ids = itertools.count(1)


class ServiceError(Exception):
    pass


# Return type for `sys.exc_info()`
ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]


class ControlSignal(enum.Enum):
    """Control signal, used to request an instance's state change from the controlling Cluster."""

    stop = "stop"


class ClusterState(statemachine.StateMachine):
    running = statemachine.State("running", initial=True)
    stopping = statemachine.State("stopping")

    stop = running.to(stopping)


class Cluster(AsyncContextManager):
    """Golem's sub-engine used to spawn and control instances of a single :class:`Service`."""

    def __init__(
        self,
        engine: "_Engine",
        payload: Payload,
        expiration: Optional[datetime] = None,
        respawn_unstarted_instances: bool = True,
        network: Optional[Network] = None,
    ):
        """Initialize this Cluster.

        :param engine: an engine for running service instance
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
    def network(self) -> Optional[Network]:
        """Return the :class:`~yapapi.network.Network` record associated with the VPN used by this :class:`Cluster`."""
        return self._network

    def __repr__(self):
        #   TODO: prettier print (cnt: service_class for every service_class)
        service_classes = set(type(service).__name__ for service in self.instances)
        service_classes = sorted(service_classes)
        return (
            f"Cluster {self.id}: {len(self.__instances)}x[Services: {service_classes}, "
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

    async def spawn_instance(self, service: Service, network_address: Optional[str] = None) -> None:
        """Spawn a new service instance within this :class:`Cluster`."""

        logger.debug("spawning instance within %s", self)
        agreement_id: Optional[str]  # set in start_worker

        instance = service.service_instance

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
                    task_data=f"Service: {type(service).__name__}",
                )
            )
            self._change_state(instance)  # pending -> starting
            service._set_ctx(work_context)
            if self.network:
                service._set_network_node(
                    await self.network.add_node(work_context.provider_id, network_address)
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

        while self._state == ClusterState.running:
            agreement_id = None
            await asyncio.sleep(1.0)
            task = await self._engine.start_worker(self._job, _worker)
            if not task:
                continue
            try:
                await task
                if (
                    instance.started_successfully
                    or not self._respawn_unstarted_instances
                    # There was no error, but rather e.g. `STOP` signal -> do not restart
                    or service.exc_info() == (None, None, None)
                ):
                    break
                else:
                    logger.warning("Instance failed when starting, trying to create another one...")
                    instance.service_state.restart()
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

    def add_instance(self, service: Service, network_address: Optional[str] = None) -> None:
        self.__instances.append(service.service_instance)
        service._set_cluster(self)

        loop = asyncio.get_event_loop()
        task = loop.create_task(self.spawn_instance(service, network_address))
        self._instance_tasks.add(task)

    def stop(self):
        """Signal the whole :class:`Cluster` to stop."""
        self.__state.stop()

        for s in self.instances:
            self.stop_instance(s)

    @property
    def _state(self):
        """Current state of the Cluster."""
        return self.__state.current_state
