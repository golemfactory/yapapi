"""An implementation of the new Golem's task executor."""
import asyncio
from asyncio import CancelledError
from datetime import datetime, timedelta, timezone
import sys
from typing import (
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
)
from typing_extensions import Final, AsyncGenerator

from yapapi import rest, events
from yapapi.ctx import WorkContext
from yapapi.payload import Payload
from yapapi.rest.activity import Activity
from yapapi.script import Script
from yapapi.engine import _Engine, Job
from yapapi.script.command import Deploy, Start
import yapapi.utils

from .task import Task, TaskStatus
from ._smartq import SmartQueue

CFG_INVOICE_TIMEOUT: Final[timedelta] = timedelta(minutes=5)
"Time to receive invoice from provider after tasks ended."

DEFAULT_EXECUTOR_TIMEOUT: Final[timedelta] = timedelta(minutes=15)
"Joint timeout for all tasks submitted to an executor."


logger = yapapi.utils.get_logger(__name__)

DEFAULT_GET_OFFERS_TIMEOUT = timedelta(seconds=20)


D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Executor:
    """Task executor.

    Used to run batch tasks using the specified application package within providers'
    execution units.
    """

    def __init__(
        self,
        *,
        _engine: _Engine,
        payload: Payload,
        implicit_init: bool,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
    ):
        logger.debug("Creating Executor instance; parameters: %s", locals())

        self._engine = _engine
        self._payload = payload
        self._implicit_init = implicit_init
        self._timeout = timeout
        self._max_workers = max_workers

    @property
    def driver(self) -> str:
        """Return the payment driver used for this `Executor`'s engine."""
        return self._engine.payment_driver

    @property
    def payment_network(self) -> str:
        """Return the payment network used for this `Executor`'s engine."""
        return self._engine.payment_network

    def emit(self, event: events.Event) -> None:
        """Emit a computation event using this `Executor`'s engine."""
        self._engine.emit(event)

    def submit(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        job_id: Optional[str] = None,
    ) -> AsyncIterator[Task[D, R]]:
        """Submit a computation to be executed on providers.

        :param worker: a callable that takes a WorkContext object and a list o tasks,
            adds commands to the context object and yields committed commands
        :param data: an iterable or an async generator iterator of Task objects to be computed
            on providers
        :param job_id: an optional string to identify the job created by this method.
            Passed as the value of the `id` parameter to `Job()`.
        :return: yields computation progress events
        """
        generator = self._create_task_generator(worker, data, job_id)
        self._engine.register_generator(generator)
        return generator

    async def _create_task_generator(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        job_id: Optional[str],
    ) -> AsyncGenerator[Task[D, R], None]:
        """Create an async generator yielding completed tasks."""

        job = Job(
            self._engine,
            expiration_time=datetime.now(timezone.utc) + self._timeout,
            payload=self._payload,
            id=job_id,
        )
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
            AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        services: Set[asyncio.Task],
        workers: Set[asyncio.Task],
        job: Job,
    ) -> AsyncGenerator[Task[D, R], None]:

        self.emit(events.ComputationStarted(job.id, job.expiration_time))

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

        async def run_worker(
            agreement: rest.market.Agreement, activity: Activity, work_context: WorkContext
        ) -> None:
            """Run an instance of `worker` for the particular activity and work context."""

            with work_queue.new_consumer() as consumer:
                try:

                    async def task_generator() -> AsyncIterator[Task[D, R]]:
                        async for handle in consumer:
                            task = Task.for_handle(handle, work_queue, self.emit)
                            self._engine.emit(
                                events.TaskStarted(
                                    job_id=job.id,
                                    agr_id=agreement.id,
                                    task_id=task.id,
                                    task_data=task.data,
                                )
                            )
                            yield task
                            self._engine.emit(
                                events.TaskFinished(
                                    job_id=job.id, agr_id=agreement.id, task_id=task.id
                                )
                            )

                    batch_generator = worker(work_context, task_generator())

                    if self._implicit_init:
                        await self._perform_implicit_init(
                            work_context, job.id, agreement.id, activity
                        )

                    try:
                        await self._engine.process_batches(
                            job.id, agreement.id, activity, batch_generator
                        )
                    except StopAsyncIteration:
                        pass
                    self.emit(events.WorkerFinished(job_id=job.id, agr_id=agreement.id))
                except Exception:
                    self.emit(
                        events.WorkerFinished(
                            job_id=job.id, agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                        )
                    )
                    raise
                finally:
                    await self._engine.accept_payments_for_agreement(job.id, agreement.id)

        async def worker_starter() -> None:
            while True:
                await asyncio.sleep(2)
                await job.agreements_pool.cycle()
                if len(workers) < self._max_workers and work_queue.has_unassigned_items():
                    new_task = None
                    try:
                        new_task = await self._engine.start_worker(job, run_worker)
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
                if now > job.expiration_time:
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

        except (Exception, CancelledError, KeyboardInterrupt) as e:
            #   TODO: why do we catch KeyboardInterrupt? How can we get one here?
            self.emit(events.ComputationFinished(job.id, exc_info=sys.exc_info()))  # type: ignore
            cancelled = True

            if isinstance(e, CancelledError):
                #   CancelledError is an "external" error, not caused by the Executor internals ->
                #   we don't want to suppress it here
                raise

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
                    logger.info(
                        "Waiting for %s to finish...",
                        pluralize(len(pending), "worker"),
                        job_id=job.id,
                    )
                    await asyncio.wait(workers, timeout=9, return_when=asyncio.ALL_COMPLETED)

            try:
                logger.debug("Terminating agreements...", job_id=job.id)
                await job.agreements_pool.terminate_all(reason=reason)
            except Exception:
                logger.debug("Problem with agreements termination", exc_info=True, job_id=job.id)

            try:
                logger.info("Waiting for Executor services to finish...", job_id=job.id)
                _, pending = await asyncio.wait(
                    workers.union(services), timeout=10, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.debug(
                        "%s still running: %s",
                        pluralize(len(pending), "service"),
                        pending,
                        job_id=job.id,
                    )
            except Exception:
                logger.debug(
                    "Got error when waiting for services to finish", exc_info=True, job_id=job.id
                )

    async def _perform_implicit_init(self, ctx, job_id, agreement_id, activity):
        async def implicit_init():
            script = ctx.new_script()
            script.add(Deploy())
            script.add(Start())
            yield script

        try:
            await self._engine.process_batches(job_id, agreement_id, activity, implicit_init())
        except StopAsyncIteration:
            pass
