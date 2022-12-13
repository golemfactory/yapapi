import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
import json
import sys
from typing import (
    TYPE_CHECKING,
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

if sys.version_info > (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

import yapapi
from yapapi import events
from yapapi.ctx import WorkContext
from yapapi.engine import _Engine
from yapapi.event_dispatcher import AsyncEventDispatcher
from yapapi.executor import Executor
from yapapi.executor.task import Task
from yapapi.network import Network
from yapapi.payload import Payload
from yapapi.props import com
from yapapi.script import Script
from yapapi.services import Cluster, ServiceType
from yapapi.strategy import (
    DecreaseScoreForUnconfirmedAgreement,
    LeastExpensiveLinearPayuMS,
)

if TYPE_CHECKING:
    from yapapi.strategy import BaseMarketStrategy


D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class _EngineKwargs(TypedDict):
    budget: Union[float, Decimal]
    strategy: "BaseMarketStrategy"
    event_consumer: Callable[[events.Event], None]
    subnet_tag: Optional[str]
    payment_driver: Optional[str]
    payment_network: Optional[str]
    stream_output: bool
    app_key: Optional[str]


class Golem:
    """The main entrypoint of Golem\\'s high-level API.

    Its principal role is providing an interface to run the requestor's payload using one of two
    modes of operation - executing tasks and running services.

    The first one, available through :func:`execute_tasks`, instructs :class:`Golem` to take a sequence of
    tasks that the user wishes to compute on Golem and distributes those among the providers.

    The second one, invoked with :func:`run_service`, makes :class:`Golem` spawn a certain number of instances
    of a service based on a single service specification (a specialized implementation
    inheriting from :class:`~yapapi.services.Service`).

    While the two modes are not necessarily completely disjoint - in that we can create a
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

    Internally, :class:`Golem`'s job includes running the engine which takes care of first finding the
    providers interested in the jobs the requestors want to execute, then negotiating agreements
    with them and facilitating the execution of those jobs and lastly, processing payments. For this
    reason, it's usually good to have just one instance of :class:`Golem` operative at any given time.
    """

    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional["yapapi.strategy.BaseMarketStrategy"] = None,
        subnet_tag: Optional[str] = None,
        payment_driver: Optional[str] = None,
        payment_network: Optional[str] = None,
        event_consumer: Optional[Callable[[events.Event], None]] = None,
        stream_output: bool = False,
        app_key: Optional[str] = None,
    ):
        """Initialize Golem engine.

        :param budget: maximum budget for payments
        :param strategy: market strategy used to select providers from the market
            (e.g. :class:`yapapi.strategy.LeastExpensiveLinearPayuMS` or :class:`yapapi.strategy.DummyMS`)
        :param subnet_tag: use only providers in the subnet with the subnet_tag name.
            Uses `YAGNA_SUBNET` environment variable, defaults to `None`
        :param payment_driver: name of the payment driver to use. Uses `YAGNA_PAYMENT_DRIVER`
            environment variable, defaults to `erc20`. Only payment platforms with
            the specified driver will be used
        :param payment_network: name of the network to use. Uses `YAGNA_NETWORK` environment
            variable, defaults to `rinkeby`. Only payment platforms with the specified
            network will be used
        :param event_consumer: a callable that processes events related to the
            computation; by default it is a function that logs all events
        :param stream_output: stream computation output from providers
        :param app_key: optional Yagna application key. If not provided, the default is to
                        get the value from `YAGNA_APPKEY` environment variable
        """
        self._event_dispatcher = AsyncEventDispatcher()

        self.add_event_consumer(event_consumer or self._default_event_consumer())

        if not strategy:
            strategy = self._initialize_default_strategy()

        self._engine_kwargs: _EngineKwargs = {
            "budget": budget,
            "strategy": strategy,
            "event_consumer": self._event_dispatcher.emit,
            "subnet_tag": subnet_tag,
            "payment_driver": payment_driver,
            "payment_network": payment_network,
            "stream_output": stream_output,
            "app_key": app_key,
        }

        self._engine: _Engine = self._get_new_engine()
        self._engine_state_lock = asyncio.Lock()

    def add_event_consumer(
        self,
        event_consumer: Callable[[events.Event], None],
        event_classes_or_names: Iterable[Union[Type[events.Event], str]] = (events.Event,),
    ):
        """Initialize another `event_consumer`, working just like the `event_consumer` passed to :func:`Golem.__init__`

        :param event_consumer: A callable that will be executed on every event.
        :param event_classes_or_names: An iterable defining classes of events that should be passed to
            this `event_consumer`. Both classes and class names are accepted (in the latter case classes must be
            available in the `yapapi.events` namespace).
            If this argument is omitted, all events inheriting from :class:`yapapi.events.Event`
            (i.e. all currently implemented events) will be passed to the `event_consumer`.

        Example usages::

            def event_consumer(event: "yapapi.events.Event"):
                print(f"Got an event! {type(event).__name__}")

            golem.add_event_consumer(event_consumer)

        ::

            def event_consumer(event: "yapapi.events.AgreementConfirmed"):
                provider_name = event.agreement.details.provider_node_info.name
                print(f"We're trading with {provider_name}! Nice!")

            golem.add_event_consumer(event_consumer, ["AgreementConfirmed"])

        """
        event_classes = set((self._parse_event_cls_or_name(x) for x in event_classes_or_names))
        self._event_dispatcher.add_event_consumer(event_consumer, event_classes, self.operative)

    @staticmethod
    def _parse_event_cls_or_name(
        event_cls_or_name: Union[Type[events.Event], str]
    ) -> Type[events.Event]:
        if isinstance(event_cls_or_name, type):
            return event_cls_or_name
        else:
            try:
                return getattr(events, event_cls_or_name)  # type: ignore
            except AttributeError:
                raise ValueError(
                    "Second argument must be either an event class, or a name of "
                    f"a class defined on `yapapi.events`, got {event_cls_or_name}"
                )

    @property
    def payment_driver(self) -> str:
        """Name of the payment driver to be used by this instance."""
        return self._engine.payment_driver

    @property
    def payment_network(self) -> str:
        """Name of the payment network to be used by this instance."""
        return self._engine.payment_network

    @property
    def strategy(self) -> "BaseMarketStrategy":
        """Return the instance of `BaseMarketStrategy` used by the engine"""
        return self._engine.strategy

    @strategy.setter
    def strategy(self, strategy: "BaseMarketStrategy") -> None:
        if self.operative:
            #   NOTE: this restriction **might** be loosened in the future,
            #         e.g. to allow "operative" Golem with an Engine that is
            #         not working on any Job
            raise AttributeError(
                "Strategy replacement is possible only when Golem is not operative"
            )

        self._engine_kwargs["strategy"] = strategy
        self._engine._strategy = strategy

    @property
    def subnet_tag(self) -> Optional[str]:
        """Return the name of the subnet, or `None` if it is not set."""
        return self._engine.subnet_tag

    @property
    def operative(self) -> bool:
        """Return True if Golem started and didn't stop"""
        engine_init_finished = hasattr(self, "_engine")  # to avoid special cases in __init__
        return engine_init_finished and self._engine.started

    async def start(self) -> None:
        """Start the Golem engine in non-contextmanager mode.

        The default way of using Golem::

            async with Golem(...) as golem:
                # ... work with golem

        Is roughly equivalent to::

            golem = Golem(...)
            try:
                await golem.start()
                # ... work with golem
            finally:
                await golem.stop()

        A repeated call to :func:`Golem.start()`:
            * If Golem is already starting, or started and wasn't stopped - will be ignored (and harmless)
            * If Golem was stopped - will initialize a new engine that knows nothing about the previous operations
        """
        try:
            async with self._engine_state_lock:
                if self.operative:
                    #   Something started us before we got to the locked part
                    return

                self._event_dispatcher.start()
                await self._engine.start()
        except:
            await self._stop_with_exc_info(*sys.exc_info())
            raise

    async def stop(self) -> None:
        """Stop the Golem engine after it was started in non-contextmanager mode.

        Details: :func:`Golem.start()`"""
        await self._stop_with_exc_info(None, None, None)

    async def __aenter__(self) -> "Golem":
        await self.start()
        return self

    async def __aexit__(self, *exc_info) -> Optional[bool]:
        return await self._stop_with_exc_info(*exc_info)

    async def _stop_with_exc_info(self, *exc_info) -> Optional[bool]:
        async with self._engine_state_lock:
            res = await self._engine.stop(*exc_info)
            await self._event_dispatcher.stop()

        #   Engine that was stopped is not usable anymore, there is no "full" cleanup.
        #   That's why here we replace it with a fresh one.
        self._engine = self._get_new_engine()

        return res

    def _get_new_engine(self):
        return _Engine(**self._engine_kwargs)

    async def execute_tasks(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        payload: Payload,
        max_workers: Optional[int] = None,
        timeout: Optional[timedelta] = None,
        job_id: Optional[str] = None,
        implicit_init: bool = True,
    ) -> AsyncIterator[Task[D, R]]:
        """Submit a sequence of tasks to be executed on providers.

        Internally, this method creates an instance of :class:`yapapi.executor.Executor`
        and calls its :func:`submit()` method with given worker function and sequence of tasks.

        :param worker: an async generator that takes a :class:`WorkContext` object and a sequence
            of tasks, and generates as sequence of scripts to be executed on providers in order
            to compute given tasks
        :param data: an iterable or an async generator of :class:`Task` objects to be computed on providers
        :param payload: specification of the payload that needs to be deployed on providers
            (for example, a VM runtime package) in order to compute the tasks, passed to
            the created :class:`Executor` instance
        :param max_workers: maximum number of concurrent workers, passed to the :class:`Executor` instance
        :param timeout: timeout for computing all tasks, passed to the :class:`Executor` instance
        :param job_id: an optional string to identify the job created by this method.
            Passed as the value of the `id` parameter to :class:`yapapi.engine.Job`.
        :param implicit_init: True -> :func:`~yapapi.script.Script.deploy()` and :func:`~yapapi.script.Script.start()`
            will be called internally by the :class:`Executor`. False -> those calls must be in the `worker` function

        :return: an async iterator that yields completed `Task` objects

        example usage::

            async def worker(context: WorkContext, tasks: AsyncIterable[Task]):
                async for task in tasks:
                    script = context.new_script()
                    future_result = script.run("/bin/sh", "-c", "date")
                    yield script
                    task.accept_result(result=await future_result)

            package = await vm.repo(
                image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
            )

            async with Golem(budget=1.0, subnet_tag="public") as golem:
                async for completed in golem.execute_tasks(worker, [Task(data=None)], payload=package):
                    print(completed.result.stdout)

        """

        kwargs: Dict[str, Any] = {"payload": payload, "implicit_init": implicit_init}
        if max_workers:
            kwargs["max_workers"] = max_workers
        if timeout:
            kwargs["timeout"] = timeout

        executor = Executor(_engine=self._engine, **kwargs)
        async for t in executor.submit(worker, data, job_id=job_id):
            yield t

    async def run_service(
        self,
        service_class: Type[ServiceType],
        num_instances: Optional[int] = None,
        instance_params: Optional[Iterable[Dict]] = None,
        payload: Optional[Payload] = None,
        expiration: Optional[datetime] = None,
        network: Optional[Network] = None,
        network_addresses: Optional[List[str]] = None,
    ) -> Cluster[ServiceType]:
        """Run a number of instances of a service represented by a given :class:`~yapapi.services.Service` subclass.

        :param service_class: a subclass of :class:`~yapapi.services.Service` that represents the service to be run
        :param num_instances: optional number of service instances to run. Defaults to a single
            instance, unless `instance_params` is given, in which case, the :class:`~yapapi.services.Cluster` will be
            created with as many instances as there are elements in the `instance_params` iterable.
            if `num_instances` is set to < 1, the :class:`~yapapi.services.Cluster` will still be created but no
            instances will be spawned within it.
        :param instance_params: optional list of dictionaries of keyword arguments that will be passed
            to consecutive, spawned instances. The number of elements in the iterable determines the
            number of instances spawned, unless `num_instances` is given, in which case the latter takes
            precedence.
            In other words, if both `num_instances` and `instance_params` are provided,
            the :class:`~yapapi.services.Cluster` will be created with the number of instances determined by
            `num_instances` and if there are too few elements in the `instance_params` iterable, it will results in
            an error.
        :param payload: optional runtime definition for the service; if not provided, the
            payload specified by the :func:`~yapapi.services.Service.get_payload()` method of `service_class` is used
        :param expiration: optional expiration datetime for the service
        :param network: optional :class:`~yapapi.network.Network`, representing a VPN to attach this
            :class:`~yapapi.services.Cluster`'s instances to
        :param network_addresses: optional list of addresses to assign to consecutive spawned instances.
            If there are too few addresses given in the `network_addresses` iterable to satisfy
            all spawned instances, the rest (or all when the list is empty or not provided at all)
            of the addresses will be assigned automatically.
            Requires the `network` argument to be provided at the same time.

        example usage::

            DATE_OUTPUT_PATH = "/golem/work/date.txt"
            REFRESH_INTERVAL_SEC = 5


            class DateService(Service):
                @staticmethod
                async def get_payload():
                    return await vm.repo(
                        image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
                    )

                async def start(self):
                    async for script in super().start():
                        yield script

                    # every `DATE_POLL_INTERVAL` write output of `date` to `DATE_OUTPUT_PATH`
                    script = self._ctx.new_script()
                    script.run(
                        "/bin/sh",
                        "-c",
                        f"while true; do date > {DATE_OUTPUT_PATH}; sleep {REFRESH_INTERVAL_SEC}; done &",
                    )
                    yield script

                async def run(self):
                    while True:
                        await asyncio.sleep(REFRESH_INTERVAL_SEC)
                        script = self._ctx.new_script()
                        future_result = script.run(
                            "/bin/sh",
                            "-c",
                            f"cat {DATE_OUTPUT_PATH}",
                        )

                        yield script

                        result = (await future_result).stdout
                        print(result.strip() if result else "")


            async def main():
                async with Golem(budget=1.0, subnet_tag="public") as golem:
                    cluster = await golem.run_service(DateService, num_instances=1)
                    start_time = datetime.now()

                    while datetime.now() < start_time + timedelta(minutes=1):
                        for num, instance in enumerate(cluster.instances):
                            print(f"Instance {num} is {instance.state.value} on {instance.provider_name}")
                        await asyncio.sleep(REFRESH_INTERVAL_SEC)

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
        """
        async with self._engine._root_api_session.get(
            f"{self._engine._api_config.root_url}/me"
        ) as resp:
            identity = json.loads(await resp.text()).get("identity")

        return await Network.create(
            self._engine._net_api, ip, identity, owner_ip, mask=mask, gateway=gateway
        )

    @staticmethod
    def _default_event_consumer() -> Callable[[events.Event], None]:
        from yapapi.log import log_event_repr, log_summary

        return log_summary(log_event_repr)

    def _initialize_default_strategy(self) -> DecreaseScoreForUnconfirmedAgreement:
        """Create a default strategy and register it's event consumer"""
        base_strategy = LeastExpensiveLinearPayuMS(
            max_fixed_price=Decimal("1.0"),
            max_price_for={com.Counter.CPU: Decimal("0.2"), com.Counter.TIME: Decimal("0.1")},
        )
        strategy = DecreaseScoreForUnconfirmedAgreement(base_strategy, 0.5)
        self.add_event_consumer(strategy.on_event)
        return strategy
