import sys
from datetime import datetime, timedelta
import json
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
    TYPE_CHECKING
)
from typing_extensions import AsyncGenerator

from yapapi import events
from yapapi.ctx import WorkContext
from yapapi.engine import _Engine, WorkItem
from yapapi.executor import Executor
from yapapi.executor.task import Task
from yapapi.network import Network
from yapapi.payload import Payload
from yapapi.services import Cluster, Service

if TYPE_CHECKING:
    from yapapi.strategy import MarketStrategy


D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Golem:
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

    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional['MarketStrategy'] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[events.Event], None]] = None,
        stream_output: bool = False,
        app_key: Optional[str] = None,
    ):
        self._init_args = {
            'budget': budget,
            'strategy': strategy,
            'subnet_tag': subnet_tag,
            'driver': driver,
            'network': network,
            'event_consumer': event_consumer,
            'stream_output': stream_output,
            'app_key': app_key,
        }

        self._engine: _Engine = self._get_new_engine()

    @property
    def driver(self) -> str:
        """Name of the payment driver"""
        return self._engine.driver

    @property
    def network(self) -> str:
        """Name of the payment network"""
        return self._engine.network

    @property
    def strategy(self) -> 'MarketStrategy':
        """Return the instance of `MarketStrategy` used by the engine"""
        return self._engine.strategy

    @property
    def subnet_tag(self) -> Optional[str]:
        """Return the name of the subnet, or `None` if it is not set."""
        return self._engine.subnet_tag

    async def __aenter__(self) -> 'Golem':
        try:
            await self._engine.start()
            return self
        except:
            await self.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, *exc_info) -> Optional[bool]:
        res = await self._engine.stop(*exc_info)

        #   Engine that was stopped is not usable anymore, there is no "full" cleanup
        #   That's why here we replace it with a fresh one
        self._engine = self._get_new_engine()
        return res

    def _get_new_engine(self):
        args = self._init_args
        return _Engine(
            budget=args['budget'],
            strategy=args['strategy'],
            subnet_tag=args['subnet_tag'],
            driver=args['driver'],
            network=args['network'],
            event_consumer=args['event_consumer'],
            stream_output=args['stream_output'],
            app_key=args['app_key'],
        )

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
        job_id: Optional[str] = None,
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
        :param job_id: an optional string to identify the job created by this method.
            Passed as the value of the `id` parameter to `Job()`.
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

        executor = Executor(_engine=self._engine, **kwargs)
        async for t in executor.submit(worker, data, job_id=job_id):
            yield t

    async def run_service(
        self,
        service_class: Type[Service],
        num_instances: Optional[int] = None,
        instance_params: Optional[Iterable[Dict]] = None,
        payload: Optional[Payload] = None,
        expiration: Optional[datetime] = None,
        respawn_unstarted_instances=True,
        network: Optional[Network] = None,
        network_addresses: Optional[List[str]] = None,
    ) -> Cluster:
        """Run a number of instances of a service represented by a given `Service` subclass.

        :param service_class: a subclass of `Service` that represents the service to be run
        :param num_instances: optional number of service instances to run. Defaults to a single
            instance, unless `instance_params` is given, in which case, the Cluster will be created
            with as many instances as there are elements in the `instance_params` iterable.
            if `num_instances` is set to < 1, the `Cluster` will still be created but no instances
            will be spawned within it.
        :param instance_params: optional list of dictionaries of keyword arguments that will be passed
            to consecutive, spawned instances. The number of elements in the iterable determines the
            number of instances spawned, unless `num_instances` is given, in which case the latter takes
            precedence.
            In other words, if both `num_instances` and `instance_params` are provided,
            the Cluster will be created with the number of instances determined by `num_instances`
            and if there are too few elements in the `instance_params` iterable, it will results in
            an error.
        :param payload: optional runtime definition for the service; if not provided, the
            payload specified by the `get_payload()` method of `service_class` is used
        :param expiration: optional expiration datetime for the service
        :param respawn_unstarted_instances: if an instance fails in the `starting` state, should
            the returned Cluster try to spawn another instance
        :param network: optional Network, representing a VPN to attach this Cluster's instances to
        :param network_addresses: optional list of addresses to assign to consecutive spawned instances.
            If there are too few addresses given in the `network_addresses` iterable to satisfy
            all spawned instances, the rest (or all when the list is empty or not provided at all)
            of the addresses will be assigned automatically.
            Requires the `network` argument to be provided at the same time.
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

        if network_addresses and not network:
            raise ValueError("`network_addresses` provided without a `network`.")

        cluster = Cluster(
            engine=self._engine,
            service_class=service_class,
            payload=payload,
            expiration=expiration,
            respawn_unstarted_instances=respawn_unstarted_instances,
            network=network,
        )

        await self._engine.add_to_async_context(cluster)
        cluster.spawn_instances(num_instances, instance_params, network_addresses)

        return cluster

    async def create_network(
        self,
        ip: str,
        owner_ip: Optional[str] = None,
        mask: Optional[str] = None,
        gateway: Optional[str] = None,
    ) -> Network:
        """
        Create a VPN inside Golem network.

        Requires yagna >= 0.8

        :param ip: the IP address of the network. May contain netmask, e.g. "192.168.0.0/24"
        :param owner_ip: the desired IP address of the requestor node within the newly-created Network
        :param mask: Optional netmask (only if not provided within the `ip` argument)
        :param gateway: Optional gateway address for the network

        :return: a Network object allowing further manipulation of the created VPN
        """
        async with self._root_api_session.get(f"{self._api_config.root_url}/me") as resp:
            identity = json.loads(await resp.text()).get("identity")

        return await Network.create(
            self._net_api, ip, identity, owner_ip, mask=mask, gateway=gateway
        )
