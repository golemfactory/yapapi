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
from yapapi.engine import _Engine, WorkItem
from yapapi.executor import Executor
from yapapi.executor.ctx import WorkContext
from yapapi.executor.task import Task
from yapapi.payload import Payload
from yapapi.services import Cluster, Service

D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Golem(_Engine):
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
    ) -> Cluster:
        """Run a number of instances of a service represented by a given `Service` subclass.

        :param service_class: a subclass of `Service` that represents the service to be run
        :param num_instances: optional number of service instances to run, defaults to a single
            instance
        :param payload: optional runtime definition for the service; if not provided, the
            payload specified by the `get_payload()` method of `service_class` is used
        :param expiration: optional expiration datetime for the service
        :return: a `Cluster` of service instances
        """

        from .services import Cluster  # avoid circular dependency

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
        )
        await self._stack.enter_async_context(cluster)
        cluster.spawn_instances()
        return cluster
