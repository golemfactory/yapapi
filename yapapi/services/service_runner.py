import asyncio
import enum
import inspect
import logging
import sys
from types import TracebackType
from typing import (
    AsyncContextManager,
    Callable,
    List,
    Optional,
    Set,
    Union,
    Tuple,
    Type,
    TYPE_CHECKING,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from yapapi.engine import Job

from yapapi import events

from .service import Service, ServiceType, ServiceInstance
from .service_state import ServiceState
from yapapi.ctx import WorkContext
from yapapi.rest.activity import BatchError
from yapapi.rest.market import Agreement
from yapapi.network import Network

# Return type for `sys.exc_info()`
ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]


class ControlSignal(enum.Enum):
    """Control signal, used to request an instance's state change from the controlling SeviceRunner."""

    stop = "stop"


class ServiceRunner(AsyncContextManager):
    def __init__(self, job: "Job"):
        self._job = job
        self._instances: List[Service] = []
        self._instance_tasks: List[asyncio.Task] = []
        self._stopped = False

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
        respawn_condition: Optional[Callable[[ServiceType], bool]] = None,
    ) -> None:
        """Add service the the collection of services managed by this ServiceRunner.

        The same object should never be managed by more than one ServiceRunner.
        """
        self._instances.append(service)

        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.spawn_instance(service, network, network_address, respawn_condition)
        )
        self._instance_tasks.append(task)

    def stop_instance(self, service: Service):
        """Stop the specific :class:`Service` instance belonging to this :class:`ServiceRunner`."""
        service.service_instance.control_queue.put_nowait(ControlSignal.stop)

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
        self._stopped = True

        logger.debug("%s is shutting down...", self)

        if exc_type is not None:
            self._job.set_exc_info((exc_type, exc_val, exc_tb))

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

        self._job.engine.finalize_job(self._job)

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

    async def spawn_instance(
        self,
        service: ServiceType,
        network: Optional[Network] = None,
        network_address: Optional[str] = None,
        respawn_condition: Optional[Callable[[ServiceType], bool]] = None,
    ) -> None:
        """Lifecycle the service within this :class:`ServiceRunner`.

        :param service: instance of the service class, expected to be in a pending state
        :param network: a :class:`~yapapi.network.Network` this service should be attached to
        :param network_address: the address withing the network, ignored if network is None
        :param respawn_condition: a bool-returning function called when a single lifecycle of the instance finished,
            determining whether service should be reset and lifecycle should restart
        """

        await self._ensure_payload_matches(service)

        logger.debug("spawning instance within %s", self)
        agreement: Optional[Agreement]  # set in start_worker

        instance = service.service_instance

        async def _worker(work_context: WorkContext) -> None:
            nonlocal instance
            assert agreement is not None

            activity = work_context._activity

            work_context.emit(events.ServiceStarted, service=service)
            self._change_state(instance)  # pending -> starting
            service._set_ctx(work_context)
            if network:
                service._set_network_node(
                    await network.add_node(work_context.provider_id, network_address)
                )
            try:
                if not self._stopped:
                    instance_batches = self._run_instance(instance)
                    try:
                        await self._job.engine.process_batches(
                            self._job.id, agreement.id, activity, instance_batches
                        )
                    except StopAsyncIteration:
                        pass

                work_context.emit(events.ServiceFinished, service=service)
                work_context.emit(events.WorkerFinished)
            except Exception:
                work_context.emit(events.WorkerFinished, exc_info=sys.exc_info())
                raise
            finally:
                await self._job.engine.accept_payments_for_agreement(self._job.id, agreement.id)
                await self._job.agreements_pool.release_agreement(agreement.id, allow_reuse=False)

        def on_agreement_ready(agreement_ready: Agreement) -> None:
            nonlocal agreement
            agreement = agreement_ready

        while not self._stopped:
            agreement = None
            await asyncio.sleep(1.0)
            task = await self._job.engine.start_worker(self._job, _worker, on_agreement_ready)
            if not task:
                continue
            try:
                await task
                if respawn_condition is not None and respawn_condition(service):
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
        #   Possible improvement: maybe we should accept services with lower demands then our payload?
        #   E.g. if service expects 2GB and we have 4GB in our payload, then this seems fine.
        #   (Not sure how much effort this requires)
        service_payload = await service.get_payload()
        our_payload = self._job.payload
        if service_payload is not None and service_payload != our_payload:
            logger.error(
                "Payload mismatch: service with {service_payload} was added to runner with {our_payload}"
            )
            raise ValueError(
                f"Only payload accepted by this service runner is {our_payload}, got {service_payload}"
            )
