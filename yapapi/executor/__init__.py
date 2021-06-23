"""An implementation of the new Golem's task executor."""
import asyncio
from asyncio import CancelledError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import sys
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
    cast,
    overload,
)
from typing_extensions import Final, AsyncGenerator
import warnings

from yapapi import rest, events
from yapapi.ctx import WorkContext
from yapapi.events import Event
from yapapi.payload import Payload
from yapapi.props import NodeInfo
from yapapi.strategy import MarketStrategy

from .task import Task, TaskStatus
from ._smartq import SmartQueue


if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack  # type: ignore


CFG_INVOICE_TIMEOUT: Final[timedelta] = timedelta(minutes=5)
"Time to receive invoice from provider after tasks ended."

DEFAULT_EXECUTOR_TIMEOUT: Final[timedelta] = timedelta(minutes=15)
"Joint timeout for all tasks submitted to an executor."


logger = logging.getLogger(__name__)


DEFAULT_GET_OFFERS_TIMEOUT = timedelta(seconds=20)


from yapapi.engine import _Engine, Job, WorkItem

D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Executor(AsyncContextManager):
    """Task executor.

    Used to run batch tasks using the specified application package within providers'
    execution units.
    """

    @overload
    def __init__(
        self,
        *,
        payload: Optional[Payload] = None,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        _engine: _Engine,
    ):
        """Initialize the `Executor` to use a specific Golem `_engine`."""

    @overload
    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        payload: Optional[Payload] = None,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
    ):
        """Initialize the `Executor` for standalone usage, with `payload` parameter."""

    @overload
    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        package: Optional[Payload] = None,
    ):
        """Initialize the `Executor` for standalone usage, with `package` parameter."""

    def __init__(
        self,
        *,
        budget: Optional[Union[float, Decimal]] = None,
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        package: Optional[Payload] = None,
        payload: Optional[Payload] = None,
        _engine: Optional[_Engine] = None,
    ):
        """Initialize an `Executor`.

        :param budget: [DEPRECATED use `Golem` instead] maximum budget for payments
        :param strategy: [DEPRECATED use `Golem` instead] market strategy used to
            select providers from the market (e.g. LeastExpensiveLinearPayuMS or DummyMS)
        :param subnet_tag: [DEPRECATED use `Golem` instead] use only providers in the
            subnet with the subnet_tag name
        :param driver: [DEPRECATED use `Golem` instead] name of the payment driver
            to use or `None` to use the default driver;
            only payment platforms with the specified driver will be used
        :param network: [DEPRECATED use `Golem` instead] name of the network
            to use or `None` to use the default network;
            only payment platforms with the specified network will be used
        :param event_consumer: [DEPRECATED use `Golem` instead] a callable that
            processes events related to the computation;
            by default it is a function that logs all events
        :param stream_output: [DEPRECATED use `Golem` instead]
            stream computation output from providers
        :param max_workers: maximum number of concurrent workers performing the computation
        :param payload: specification of payload (for example a VM package) that needs to be
            deployed on providers in order to compute tasks with this Executor
        :param timeout: timeout for the whole computation
        """
        logger.debug("Creating Executor instance; parameters: %s", locals())
        self.__standalone = False

        if _engine:
            self._engine = _engine
        else:
            warnings.warn(
                "Stand-alone usage of `Executor` is deprecated, "
                "please use `Golem.execute_task` instead.",
                DeprecationWarning,
            )
            if not budget:
                raise ValueError("Missing value for `budget` argument.")

            self._engine = _Engine(
                budget=budget,
                strategy=strategy,
                subnet_tag=subnet_tag,
                driver=driver,
                network=network,
                event_consumer=event_consumer,
                stream_output=stream_output,
            )
            self.__standalone = True

        if package:
            if payload:
                raise ValueError("Cannot use `payload` and `package` at the same time")
            logger.warning(
                f"`package` argument to `{self.__class__}` is deprecated,"
                " please use `payload` instead"
            )
            payload = package
        if not payload:
            raise ValueError("Executor `payload` must be specified")

        self._payload = payload
        self._timeout: timedelta = timeout
        self._max_workers = max_workers
        self._stack = AsyncExitStack()

    @property
    def driver(self) -> str:
        """Return the payment driver used for this `Executor`'s engine."""
        return self._engine.driver

    @property
    def network(self) -> str:
        """Return the payment network used for this `Executor`'s engine."""
        return self._engine.network

    async def __aenter__(self) -> "Executor":
        """Start computation using this `Executor`."""
        if self.__standalone:
            await self._stack.enter_async_context(self._engine)
        self._expires = datetime.now(timezone.utc) + self._timeout
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release resources used by this `Executor`."""
        await self._stack.aclose()

    def emit(self, event: events.Event) -> None:
        """Emit a computation event using this `Executor`'s engine."""
        self._engine.emit(event)

    def submit(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[WorkItem, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
    ) -> AsyncIterator[Task[D, R]]:
        """Submit a computation to be executed on providers.

        :param worker: a callable that takes a WorkContext object and a list o tasks,
            adds commands to the context object and yields committed commands
        :param data: an iterable or an async generator iterator of Task objects to be computed
            on providers
        :return: yields computation progress events
        """
        generator = self._create_task_generator(worker, data)
        self._engine.register_generator(generator)
        return generator

    async def _create_task_generator(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[WorkItem, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
    ) -> AsyncGenerator[Task[D, R], None]:
        """Create an async generator yielding completed tasks."""

        job = Job(self._engine, expiration_time=self._expires, payload=self._payload)
        self._engine.add_job(job)

        services: Set[asyncio.Task] = set()
        workers: Set[asyncio.Task] = set()

        try:
            generator = self._submit(worker, data, services, workers, job)
            try:
                async for result in generator:
                    yield result
            except GeneratorExit:
                logger.debug("Early exit from submit(), cancelling the computation")
                try:
                    # Cancel `generator`. It should then exit by raising `StopAsyncIteration`.
                    await generator.athrow(CancelledError)
                except StopAsyncIteration:
                    pass
        finally:
            # Cancel and gather all tasks to make sure all exceptions are retrieved.
            all_tasks = workers.union(services)
            for task in all_tasks:
                if not task.done():
                    logger.debug("Cancelling task: %s", task)
                    task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            # Signal that this computation is finished
            self._engine.finalize_job(job)

    async def _submit(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[WorkItem, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        services: Set[asyncio.Task],
        workers: Set[asyncio.Task],
        job: Job,
    ) -> AsyncGenerator[Task[D, R], None]:

        self.emit(events.ComputationStarted(job.id, self._expires))

        done_queue: asyncio.Queue[Task[D, R]] = asyncio.Queue()

        def on_task_done(task: Task[D, R], status: TaskStatus) -> None:
            """Callback run when `task` is accepted or rejected."""
            if status == TaskStatus.ACCEPTED:
                done_queue.put_nowait(task)

        async def input_tasks() -> AsyncIterator[Task[D, R]]:
            if isinstance(data, AsyncIterator):
                async for task in data:
                    task._add_callback(on_task_done)
                    yield task
            else:
                for task in data:
                    task._add_callback(on_task_done)
                    yield task

        work_queue = SmartQueue(input_tasks())

        last_wid = 0

        async def start_worker(agreement: rest.market.Agreement, node_info: NodeInfo) -> None:

            nonlocal last_wid
            wid = last_wid
            last_wid += 1

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

                self.emit(events.ActivityCreated(act_id=act.id, agr_id=agreement.id))
                self._engine.accept_debit_notes_for_agreement(agreement.id)
                work_context = WorkContext(
                    f"worker-{wid}", node_info, self._engine.storage_manager, emitter=self.emit
                )

                with work_queue.new_consumer() as consumer:
                    try:

                        async def task_generator() -> AsyncIterator[Task[D, R]]:
                            async for handle in consumer:
                                task = Task.for_handle(handle, work_queue, self.emit)
                                self._engine.emit(
                                    events.TaskStarted(
                                        agr_id=agreement.id, task_id=task.id, task_data=task.data
                                    )
                                )
                                yield task
                                self._engine.emit(
                                    events.TaskFinished(agr_id=agreement.id, task_id=task.id)
                                )

                        batch_generator = worker(work_context, task_generator())
                        try:
                            await self._engine.process_batches(agreement.id, act, batch_generator)
                        except StopAsyncIteration:
                            pass
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

        async def worker_starter() -> None:
            while True:
                await asyncio.sleep(2)
                await job.agreements_pool.cycle()
                if len(workers) < self._max_workers and work_queue.has_unassigned_items():
                    new_task = None
                    try:
                        new_task = await job.agreements_pool.use_agreement(
                            lambda agreement, node: loop.create_task(start_worker(agreement, node))
                        )
                        if new_task is None:
                            continue
                        workers.add(new_task)
                    except CancelledError:
                        raise
                    except Exception:
                        if new_task:
                            new_task.cancel()
                        logger.debug("There was a problem during use_agreement", exc_info=True)

        loop = asyncio.get_event_loop()
        find_offers_task = loop.create_task(job.find_offers())

        wait_until_done = loop.create_task(work_queue.wait_until_done())
        worker_starter_task = loop.create_task(worker_starter())

        # Py38: find_offers_task.set_name('find_offers_task')

        get_offers_deadline = datetime.now(timezone.utc) + DEFAULT_GET_OFFERS_TIMEOUT
        get_done_task: Optional[asyncio.Task] = None
        services.update(
            {
                find_offers_task,
                wait_until_done,
                worker_starter_task,
            }
        )
        cancelled = False

        try:
            while wait_until_done in services or not done_queue.empty():

                now = datetime.now(timezone.utc)
                if now > self._expires:
                    raise TimeoutError(f"Computation timed out after {self._timeout}")
                if now > get_offers_deadline and job.proposals_confirmed == 0:
                    self.emit(
                        events.NoProposalsConfirmed(
                            num_offers=job.offers_collected, timeout=DEFAULT_GET_OFFERS_TIMEOUT
                        )
                    )
                    get_offers_deadline += DEFAULT_GET_OFFERS_TIMEOUT

                if not get_done_task:
                    get_done_task = loop.create_task(done_queue.get())
                    services.add(get_done_task)

                done, pending = await asyncio.wait(
                    services.union(workers), timeout=10, return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    # if an exception occurred when a service task was running
                    if task in services and not task.cancelled() and task.exception():
                        raise cast(BaseException, task.exception())
                    if task in workers:
                        try:
                            await task
                        except Exception:
                            pass

                workers -= done
                services -= done

                assert get_done_task
                if get_done_task.done():
                    yield get_done_task.result()
                    assert get_done_task not in services
                    get_done_task = None

            self.emit(events.ComputationFinished(job.id))

        except (Exception, CancelledError, KeyboardInterrupt):
            self.emit(events.ComputationFinished(job.id, exc_info=sys.exc_info()))  # type: ignore
            cancelled = True

        finally:

            await work_queue.close()

            # Importing this at the beginning would cause circular dependencies
            from ..log import pluralize

            for task in services:
                task.cancel()
            if cancelled:
                reason = {"message": "Work cancelled", "golem.requestor.code": "Cancelled"}
                for worker_task in workers:
                    worker_task.cancel()
            else:
                reason = {
                    "message": "Successfully finished all work",
                    "golem.requestor.code": "Success",
                }

            if workers:
                _, pending = await asyncio.wait(
                    workers, timeout=1, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.info("Waiting for %s to finish...", pluralize(len(pending), "worker"))
                    await asyncio.wait(workers, timeout=9, return_when=asyncio.ALL_COMPLETED)

            try:
                logger.debug("Terminating agreements...")
                await job.agreements_pool.terminate_all(reason=reason)
            except Exception:
                logger.debug("Problem with agreements termination", exc_info=True)

            try:
                logger.info("Waiting for Executor services to finish...")
                _, pending = await asyncio.wait(
                    workers.union(services), timeout=10, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.debug(
                        "%s still running: %s", pluralize(len(pending), "service"), pending
                    )
            except Exception:
                logger.debug("Got error when waiting for services to finish", exc_info=True)
