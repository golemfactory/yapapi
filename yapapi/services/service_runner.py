import asyncio
import enum
import inspect
import logging
import sys
from types import TracebackType
from typing import TYPE_CHECKING, AsyncContextManager, Dict, List, Optional, Set, Tuple, Type, Union

from typing_extensions import Final

if TYPE_CHECKING:
    from yapapi.engine import Job

from yapapi import events
from yapapi.ctx import WorkContext
from yapapi.network import Network, Node
from yapapi.rest.activity import BatchError
from yapapi.rest.market import Agreement

from .service import Service, ServiceInstance, ServiceType
from .service_state import ServiceState

logger = logging.getLogger(__name__)

# Return type for `sys.exc_info()`
ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]

DEFAULT_HEALTH_CHECK_INTERVAL: Final[float] = 10.0
DEFAULT_HEALTH_CHECK_RETRIES: Final[int] = 3


class ServiceRunnerError(Exception):
    """An error while running a Service."""


class ControlSignal(enum.Enum):
    """Control signal, used to request an instance's state change from the controlling \
    SeviceRunner."""

    stop = "stop"
    suspend = "suspend"


class ServiceRunner(AsyncContextManager):
    def __init__(
        self,
        job: "Job",
        health_check_interval: Optional[float] = DEFAULT_HEALTH_CHECK_INTERVAL,
        health_check_retries: int = DEFAULT_HEALTH_CHECK_RETRIES,
    ):
        """Initialize the ServiceRunner.

        :param job: the engine's :class:`~yapapi.engine.Job` within which the ServiceRunner will run
        :param health_check_interval: an interval in seconds between subsequent health checks.
            Setting it to `None` turns off the health check.
        :param health_check_retries: number of times the health check will be retried before
            it's reported as failed
        """
        self._job = job
        self._instances: List[Service] = []
        self._instance_tasks: List[asyncio.Task] = []
        self._stopped = False
        self._health_check_interval = health_check_interval
        self._health_check_retries = health_check_retries

    @property
    def id(self) -> str:
        return self._job.id

    @property
    def instances(self):
        return self._instances.copy()

    def add_instance(
        self,
        service: ServiceType,
        network: Optional[Network] = None,
        network_address: Optional[str] = None,
    ) -> None:
        """Add service to the collection of services managed by this ServiceRunner.

        The same object should never be managed by more than one ServiceRunner.
        """
        self._instances.append(service)

        loop = asyncio.get_event_loop()
        task = loop.create_task(self.spawn_instance(service, network, network_address))
        self._instance_tasks.append(task)

    def add_existing_instance(
        self,
        service: ServiceType,
        state: str,
        agreement_id: str,
        activity_id: str,
        network: Optional[Network] = None,
        network_node_dict = Optional[Dict[str, str]],
    ) -> None:
        """Add an existing service to the collection of services managed by this ServiceRunner.

        The same object should never be managed by more than one ServiceRunner.
        """

        service.service_instance.service_state.current_state_value = state

        if network_node_dict:
            service._set_network_node(
                Node(
                    network=network,
                    node_id=network_node_dict.get("node_id"),
                    ip=network_node_dict.get("ip")
                )
            )

        self._instances.append(service)

        loop = asyncio.get_event_loop()
        task = loop.create_task(self.spawn_instance(service, network, existing_agreement_id=agreement_id, existing_activity_id=activity_id))
        self._instance_tasks.append(task)

    def stop_instance(self, service: Service):
        """Stop the specific :class:`Service` instance belonging to this :class:`ServiceRunner`."""
        service.service_instance.control_queue.put_nowait(ControlSignal.stop)

    def suspend_instance(self, service: Service):
        """Suspend the specific :class:`Service` instance belonging to this :class:`ServiceRunner`."""
        service.service_instance.control_queue.put_nowait(ControlSignal.suspend)

    async def __aenter__(self):
        """Post a Demand and start collecting provider Offers for running service instances."""

        self.__services: Set[asyncio.Task] = set()
        """Asyncio tasks running within this cluster"""

        self._job.engine.add_job(self._job)

        logger.debug("Starting new %s", self)

        loop = asyncio.get_event_loop()
        task = loop.create_task(self._job.find_offers())

        def raise_if_failed(task):
            if not task.cancelled() and task.exception():
                raise task.exception()

        task.add_done_callback(raise_if_failed)
        self.__services.add(task)

        async def agreements_pool_cycler():
            # shouldn't this be part of the Agreement pool itself? (or a task within Job?)
            while True:
                await asyncio.sleep(2)
                await self._job.agreements_pool.cycle()

        self.__services.add(loop.create_task(agreements_pool_cycler()))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release resources used by this ServiceRunner."""
        print("----------------- ServiceRunner __aexit__")
        self._stopped = True

        print("----------------- ServiceRunner __aexit__ - stopped")

        logger.debug("%s is shutting down...", self)

        if exc_type is not None:
            self._job.set_exc_info((exc_type, exc_val, exc_tb))

        # Give the instance tasks some time to terminate gracefully.
        # Then cancel them without mercy!
        if self._instance_tasks:
            print("----------------- ServiceRunner __aexit__ - awaiting instance tasks termination")
            logger.debug("Waiting for service instances to terminate...")
            _, still_running = await asyncio.wait(self._instance_tasks, timeout=10)
            if still_running:
                for task in still_running:
                    print("----------------- ServiceRunner __aexit__ - cancelling task", task)
                    logger.debug("Cancelling task: %s", task)
                    task.cancel()
                await asyncio.gather(*still_running, return_exceptions=True)

        # TODO: should be different if we stop due to an error
        termination_reason = {
            "message": "Successfully finished all work",
            "golem.requestor.code": "Success",
        }

        print("----------------- ServiceRunner __aexit__ - terminate agreements")
        try:
            logger.debug("Terminating agreements...")
            await self._job.agreements_pool.terminate_all(reason=termination_reason)
        except Exception:
            logger.debug("Couldn't terminate agreements", exc_info=True)

        for task in self.__services:
            if not task.done():
                print("----------------- ServiceRunner __aexit__ - cancelling task", task)
                logger.debug("Cancelling task: %s", task)
                task.cancel()
        await asyncio.gather(*self.__services, return_exceptions=True)

        self._job.engine.finalize_job(self._job)

    @staticmethod
    def _get_handler(instance: ServiceInstance):
        _handlers = {
            ServiceState.starting: instance.service.start,
            ServiceState.running: instance.service.run,
            ServiceState.stopping: instance.service.shutdown,
            ServiceState.suspended: None,
        }
        handler = _handlers.get(instance.state, None)
        if handler:
            if inspect.isasyncgenfunction(handler):
                return handler()
            else:
                service_cls_name = type(instance.service).__name__
                handler_name = handler.__name__
                raise TypeError(
                    f"Service handler: `{service_cls_name}.{handler_name}` must be an asynchronous"
                    " generator."
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
        elif event == ControlSignal.suspend:
            instance.service_state.suspend()
        else:
            # Unhandled signal, don't change the state
            assert isinstance(event, ControlSignal)
            logger.warning("Don't know how to handle control signal %s", event)

        if isinstance(event, tuple):
            # TODO resolve issue with access to a protected attribute
            instance.service._exc_info = event

        if instance.state != prev_state:
            ctx = instance.service._ctx
            assert ctx is not None  # This is None only while pending for the first time
            ctx.emit(
                events.ServiceStateChanged, service=instance, old=prev_state, new=instance.state
            )

        return instance.state != prev_state

    async def _ensure_alive(self, service: Service):
        # wait indefinitely when the interval is not defined
        if self._health_check_interval is None:
            await asyncio.Future()
            return

        retries_left = self._health_check_retries
        while True:
            if service.is_available:
                if await service.is_activity_responsive():
                    retries_left = self._health_check_retries
                else:
                    retries_left -= 1
                    logger.warning("Service health check failed, retries left: %s", retries_left)
            if retries_left <= 0:
                raise ServiceRunnerError(
                    "Service health check failed after %s retries",
                    self._health_check_retries,
                )
            await asyncio.sleep(self._health_check_interval)

    async def _run_instance(self, instance: ServiceInstance):
        loop = asyncio.get_event_loop()

        if instance.state == ServiceState.starting:
            logger.info("%s commissioned", instance.service)
        else:
            logger.info("%s resumed", instance.service)

        handler = None
        batch_task: Optional[asyncio.Task] = None
        signal_task: Optional[asyncio.Task] = None
        health_check_task: Optional[asyncio.Task] = None

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
            if health_check_task is None:
                health_check_task = loop.create_task(self._ensure_alive(instance.service))

            done, _ = await asyncio.wait(
                (batch_task, signal_task, health_check_task),
                return_when=asyncio.FIRST_COMPLETED,
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
                    print(
                        "----------------- ServiceRunner _run_instance ---- StopAsyncIteration, state: ",
                        instance.state,
                    )
                    change_state()

                    # work-around an issue preventing nodes from getting correct information about
                    # each other on instance startup by re-sending the information after the service
                    # transitions to the running state
                    if instance.service.network and instance.state == ServiceState.running:
                        await instance.service.network.refresh_nodes()

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

            if health_check_task in done:
                try:
                    health_check_task.result()
                except ServiceRunnerError as exception:
                    logger.info("Health check task aborted: %s", exception)
                    change_state(sys.exc_info())
                    health_check_task = None

        logger.debug("No handler for %s in state %s", instance.service, instance.state.value)

        try:
            for t in [batch_task, signal_task, health_check_task]:
                if t is not None:
                    t.cancel()
                    await t
        except asyncio.CancelledError:
            print("----------------- ServiceRunner _run_instance ---- CancelledError")

        logger.info("%s decommissioned", instance.service)

    async def spawn_instance(
        self,
        service: ServiceType,
        network: Optional[Network] = None,
        network_address: Optional[str] = None,
        existing_agreement_id: Optional[str] = None,
        existing_activity_id: Optional[str] = None,

    ) -> None:
        """Lifecycle the service within this :class:`ServiceRunner`.

        :param service: instance of the service class
        :param network: a :class:`~yapapi.network.Network` this service should be attached to
        :param network_address: the address withing the network, ignored if network is None
            determining whether service should be reset and lifecycle should restart
        """

        await self._ensure_payload_matches(service)

        logger.debug("spawning instance within %s", self)
        agreement: Optional[Agreement]  # set in start_worker

        instance = service.service_instance

        async def _worker(work_context: WorkContext) -> bool:
            nonlocal instance
            assert agreement is not None

            print("----------------- ServiceRunner spawn_instance worker --- start")

            activity = work_context._activity

            service._set_ctx(work_context)

            print("----------------- ServiceRunner spawn_instance worker --- state ", instance.service_state, instance.state, instance.state == ServiceState.pending)

            if instance.state == ServiceState.pending:
                self._change_state(instance)  # pending -> starting

            print("----------------- ServiceRunner spawn_instance worker --- state2 ", instance.service_state, )

            try:
                if network and not service.network_node:
                    service._set_network_node(
                        await network.add_node(work_context.provider_id, network_address)
                    )
                if not self._stopped:
                    instance_batches = self._run_instance(instance)
                    try:
                        print(
                            "----------------- ServiceRunner spawn_instance worker --- await processbatches"
                        )
                        await self._job.engine.process_batches(
                            self._job.id, agreement.id, activity, instance_batches
                        )
                        print(
                            "----------------- ServiceRunner spawn_instance worker --- after processbatches"
                        )
                    except StopAsyncIteration:
                        print(
                            "----------------- ServiceRunner spawn_instance worker --- stopasynciteration"
                        )

                work_context.emit(events.ServiceFinished, service=service)
                work_context.emit(events.WorkerFinished)
            except Exception:
                print(
                    "----------------- ServiceRunner spawn_instance worker --- exception",
                    sys.exc_info(),
                )
                work_context.emit(events.WorkerFinished, exc_info=sys.exc_info())
                raise
            finally:
                if service.state != ServiceState.suspended:
                    print("----------------- ServiceRunner spawn_instance worker --- finally", service.state)
                    if network and service.network_node:
                        await network.remove_node(work_context.provider_id)
                        service._clear_network_node()
                    print("----------------- ServiceRunner spawn_instance worker --- accept payments")
                    await self._job.engine.accept_payments_for_agreement(self._job.id, agreement.id)
                    print("----------------- ServiceRunner spawn_instance worker --- release agreement")
                    await self._job.agreements_pool.release_agreement(agreement.id, allow_reuse=False)

                    # keep activity?
                    return False

                # keep activity?
                return True

        def on_agreement_ready(agreement_ready: Agreement) -> None:
            nonlocal agreement
            agreement = agreement_ready

        while not self._stopped:
            agreement = None
            await asyncio.sleep(1.0)
            task = await self._job.engine.start_worker(
                self._job,
                _worker,
                on_agreement_ready,
                existing_agreement_id=existing_agreement_id,
                existing_activity_id=existing_activity_id
            )
            if not task:
                continue
            try:
                await task
                if service.restart_condition:
                    logger.info(f"Restarting service {service}")
                    await service.reset()
                    instance.service_state.restart()
                else:
                    break
            except Exception:
                if not agreement:
                    # This shouldn't happen, we may log and return as well
                    logger.error("Failed to spawn instance", exc_info=True)
                    return

    async def _ensure_payload_matches(self, service: Service):
        #   Possible improvement: maybe we should accept services with lower demands then our
        #   payload? E.g. if service expects 2GB and we have 4GB in our payload, then this seems
        #   fine. (Not sure how much effort this requires)
        service_payload = await service.get_payload()
        our_payload = self._job.payload
        if service_payload is not None and service_payload != our_payload:
            logger.error(
                f"Payload mismatch: service with {service_payload} was added to runner"
                f" with {our_payload}"
            )
            raise ValueError(
                f"Only payload accepted by this service runner is {our_payload},"
                f" got {service_payload}"
            )
