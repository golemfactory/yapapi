from datetime import datetime, timedelta
from decimal import Decimal
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)
from typing_extensions import AsyncGenerator

from yapapi import events
from yapapi.ctx import WorkContext
from yapapi.engine import _Engine, WorkItem
from yapapi.executor import Executor
from yapapi.executor.task import Task
from yapapi.payload import Payload
from yapapi.services import Cluster, Service

D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Golem(_Engine):
    """The main entrypoint of Golem's high-level API.

    Provides two methods that reflect the two modes of operation, or two types of jobs
    that can currently be executed on the Golem network.

    The first one - `execute_tasks` - instructs `Golem` to take a sequence of tasks
    that the user wishes to compute on Golem and distributes those among the providers.

    The second one - `run_service` - instructs `Golem` to spawn a certain number of instances
    of a service based on a single service specification (a specialized implementation
    inheriting from `yapapi.Service`).

    While the two models are not necessarily completely disjoint - in that we can create a
    service that exists to process a certain number of computations and, similarly, we can
    use the task model to run some service - the main difference lies in the lifetime of
    such a job.

    Whereas a task-based job exists for the purpose of computing the specific
    sequence of tasks and is done once the whole sequence has been processed, the
    service-based job is created for a potentially indefinite period and the services
    spawned within it are kept alive for as long as they're needed.

    Additionally, the service interface provides a way to easily define handlers for
    certain, discrete phases of a lifetime of each service instance - startup, running and
    shutdown.

    As `Golem`'s job includes tracking and executing payments for activities spawned by
    either mode of operation, it's usually good to have just one instance of `Golem` active
    at any given time.
    """

    async def execute_tasks(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[WorkItem, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        payload: Payload,
        max_workers: Optional[int] = None,
        timeout: Optional[timedelta] = None,
        budget: Optional[Union[float, Decimal]] = None,
    ) -> AsyncIterator[Task[D, R]]:
        """Submit a sequence of tasks to be executed on providers.

        Internally, this method creates an instance of `yapapi.executor.Executor`
        and calls its `submit()` method with given worker function and sequence of tasks.

        :param worker: an async generator that takes a `WorkContext` object and a sequence
            of tasks, and generates as sequence of work items to be executed on providers in order
            to compute given tasks
        :param data: an iterable or an async generator of `Task` objects to be computed on providers
        :param payload: specification of the payload that needs to be deployed on providers
            (for example, a VM runtime package) in order to compute the tasks, passed to
            the created `Executor` instance
        :param max_workers: maximum number of concurrent workers, passed to the `Executor` instance
        :param timeout: timeout for computing all tasks, passed to the `Executor` instance
        :param budget: budget for computing all tasks, passed to the `Executor` instance
        :return: an iterator that yields completed `Task` objects

        example usage:

        ```python
            async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
                async for task in tasks:
                    context.run("/bin/sh", "-c", "date")

                    future_results = yield context.commit()
                    results = await future_results
                    task.accept_result(result=results[-1])

            package = await vm.repo(
                image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
            )

            async with Golem(budget=1.0, subnet_tag="devnet-beta.2") as golem:
                async for completed in golem.execute_tasks(worker, [Task(data=None)], payload=package):
                    print(completed.result.stdout)
        ```
        """

        kwargs: Dict[str, Any] = {"payload": payload}
        if max_workers:
            kwargs["max_workers"] = max_workers
        if timeout:
            kwargs["timeout"] = timeout
        kwargs["budget"] = budget if budget is not None else self._budget_amount

        async with Executor(_engine=self, **kwargs) as executor:
            async for t in executor.submit(worker, data):
                yield t

    async def run_service(
        self,
        service_class: Type[Service],
        num_instances: int = 1,
        payload: Optional[Payload] = None,
        expiration: Optional[datetime] = None,
        respawn_unstarted_instances=True,
    ) -> Cluster:
        """Run a number of instances of a service represented by a given `Service` subclass.

        :param service_class: a subclass of `Service` that represents the service to be run
        :param num_instances: optional number of service instances to run, defaults to a single
            instance
        :param payload: optional runtime definition for the service; if not provided, the
            payload specified by the `get_payload()` method of `service_class` is used
        :param expiration: optional expiration datetime for the service
        :param respawn_unstarted_instances: if an instance fails in the `starting` state, should
            the returned Cluster try to spawn another instance
        :return: a `Cluster` of service instances

        example usage:

        ```python
        DATE_OUTPUT_PATH = "/golem/work/date.txt"
        REFRESH_INTERVAL_SEC = 5


        class DateService(Service):
            @staticmethod
            async def get_payload():
                return await vm.repo(
                    image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
                )

            async def start(self):
                # every `DATE_POLL_INTERVAL` write output of `date` to `DATE_OUTPUT_PATH`
                self._ctx.run(
                    "/bin/sh",
                    "-c",
                    f"while true; do date > {DATE_OUTPUT_PATH}; sleep {REFRESH_INTERVAL_SEC}; done &",
                )
                yield self._ctx.commit()

            async def run(self):
                while True:
                    await asyncio.sleep(REFRESH_INTERVAL_SEC)
                    self._ctx.run(
                        "/bin/sh",
                        "-c",
                        f"cat {DATE_OUTPUT_PATH}",
                    )

                    future_results = yield self._ctx.commit()
                    results = await future_results
                    print(results[0].stdout.strip())


        async def main():
            async with Golem(budget=1.0, subnet_tag="devnet-beta.2") as golem:
                cluster = await golem.run_service(DateService, num_instances=1)
                start_time = datetime.now()

                while datetime.now() < start_time + timedelta(minutes=1):
                    for num, instance in enumerate(cluster.instances):
                        print(f"Instance {num} is {instance.state.value} on {instance.provider_name}")
                    await asyncio.sleep(REFRESH_INTERVAL_SEC)
        ```
        """
        payload = payload or await service_class.get_payload()

        if not payload:
            raise ValueError(
                f"No payload returned from {service_class.__name__}.get_payload()"
                " nor given in the `payload` argument."
            )

        cluster = Cluster(
            engine=self,
            service_class=service_class,
            payload=payload,
            num_instances=num_instances,
            expiration=expiration,
            respawn_unstarted_instances=respawn_unstarted_instances,
        )
        await self._stack.enter_async_context(cluster)
        cluster.spawn_instances()
        return cluster
