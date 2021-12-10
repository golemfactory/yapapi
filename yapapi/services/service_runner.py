import asyncio
import enum
import inspect
import logging
import sys
from types import TracebackType
from typing import AsyncContextManager, List, Optional, Set, Union, Tuple, Type, TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from yapapi.engine import Job

#   TODO: import Agreement, not whole rest
from yapapi import rest, events

from .service import Service, ServiceInstance
from .service_state import ServiceState
from yapapi.ctx import WorkContext
from yapapi.rest.activity import Activity, BatchError
from yapapi.network import Network

# Return type for `sys.exc_info()`
ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]


class ControlSignal(enum.Enum):
    """Control signal, used to request an instance's state change from the controlling Cluster."""

    stop = "stop"


class ServiceRunner(AsyncContextManager):
    def __init__(self, job: "Job"):
        self._job = job
        self._instances: List[Service] = []
        self._instance_tasks: List[asyncio.Task] = []
        self._operative = False

    @property
    def id(self) -> str:
        return self._job.id

    @property
    def operative(self):
        return self._operative

    @property
    def instances(self):
        return self._instances.copy()

    def stop(self):
        for instance in self.instances:
            self.stop_instance(instance)
        self._operative = False

    def stop_instance(self, service: Service):
        """Stop the specific :class:`Service` instance belonging to this :class:`Cluster`."""
        service.service_instance.control_queue.put_nowait(ControlSignal.stop)

    def emit(self, event: events.Event):
        #   TODO: this will change in #760
        return self._job.engine.emit(event)

    async def __aenter__(self):
        """Post a Demand and start collecting provider Offers for running service instances."""

        self.__services: Set[asyncio.Task] = set()
        """Asyncio tasks running within this cluster"""

        self._job.engine.add_job(self._job)

        logger.debug("Starting new %s", self)

        loop = asyncio.get_event_loop()
        self.__services.add(loop.create_task(self._job.find_offers()))

        async def agreements_pool_cycler():
            # shouldn't this be part of the Agreement pool itself? (or a task within Job?)
            while True:
                await asyncio.sleep(2)
                await self._job.agreements_pool.cycle()

        self.__services.add(loop.create_task(agreements_pool_cycler()))
        self._operative = True

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release resources used by this Cluster."""
        self._operative = False

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
        service: Service,
        network: Optional[Network] = None,
        network_address: Optional[str] = None,
    ) -> None:
        """Spawn a new service instance within this :class:`Cluster`."""

        logger.debug("spawning instance within %s", self)
        agreement_id: Optional[str]  # set in start_worker

        instance = service.service_instance

        async def _worker(
            agreement: rest.market.Agreement, activity: Activity, work_context: WorkContext
        ) -> None:
            nonlocal agreement_id, instance
            agreement_id = agreement.id

            task_id = service.id  # TODO -> after #759 there will be no events.Task*
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
            if network:
                service._set_network_node(
                    await network.add_node(work_context.provider_id, network_address)
                )
            try:
                if self.operative:
                    instance_batches = self._run_instance(instance)
                    try:
                        await self._job.engine.process_batches(
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
                await self._job.engine.accept_payments_for_agreement(self._job.id, agreement.id)
                await self._job.agreements_pool.release_agreement(agreement.id, allow_reuse=False)

        while self.operative:
            agreement_id = None
            await asyncio.sleep(1.0)
            task = await self._job.engine.start_worker(self._job, _worker)
            if not task:
                continue
            try:
                await task
                if (
                    instance.started_successfully
                    # TODO: implement respawning unstarted instances in Cluster
                    # or not self._respawn_unstarted_instances
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

    def add_instance(
        self,
        service: Service,
        network: Optional[Network] = None,
        network_address: Optional[str] = None,
    ) -> None:
        self._instances.append(service)

        loop = asyncio.get_event_loop()
        task = loop.create_task(self.spawn_instance(service, network, network_address))
        self._instance_tasks.append(task)
