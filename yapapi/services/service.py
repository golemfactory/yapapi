import asyncio
from dataclasses import dataclass, field
import uuid
import statemachine  # type: ignore
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

from yapapi.ctx import WorkContext
from yapapi.network import Network, Node
from yapapi.payload import Payload
from yapapi.script import Script
from yapapi import events
from yapapi.utils import warn_deprecated_msg

from .service_state import ServiceState

if TYPE_CHECKING:
    from .cluster import Cluster
    from .service_runner import ControlSignal

# Return type for `sys.exc_info()`
ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]


@dataclass
class ServiceSignal:
    """Simple container to carry information between the client code and the Service instance."""

    message: Any
    response_to: Optional["ServiceSignal"] = None


class Service:
    """Base class for service specifications.

    To be extended by application developers to define their own, specialized
    Service specifications.
    """

    _cluster: Optional["Cluster"] = None
    _ctx: Optional["WorkContext"] = None
    _network_node: Optional[Node] = None

    def __init__(self):
        self.__id = str(uuid.uuid4())

        self.__inqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()
        self.__outqueue: asyncio.Queue[ServiceSignal] = asyncio.Queue()

        # Information on exception that caused the service change state,
        # as returned by `sys.exc_info()`.
        # Tuple of `None`'s means that the transition was not caused by an exception.
        self._exc_info: ExcInfo = (None, None, None)

        self.__service_instance = ServiceInstance(self)

        # TODO: maybe transition due to a control signal should also set this? To distinguish
        # stopping the service externally (e.g., via `cluster.stop()`) from internal transition
        # (e.g., via returning from `Service.run()`).

    @property
    def cluster(self) -> Optional["Cluster"]:
        """Return the Cluster to which this service instance belongs."""
        #   TODO: is this necessary? This is ugly, because there is no reason for `Service` to know
        #         that anything like Cluster exists.
        #         Maybe `ServiceRunner`? This makes more sense.
        return self._cluster

    @property
    def id(self) -> str:
        """Return the unique id of this service instance."""
        return self.__id

    @property
    def provider_name(self) -> Optional[str]:
        """Return the name of the provider that runs this service instance."""
        if self._ctx is None:
            return None
        return self._ctx.provider_name

    @property
    def provider_id(self) -> Optional[str]:
        """Return the id of the provider that runs this service instance."""
        if self._ctx is None:
            return None
        return self._ctx.provider_id

    @property
    def network(self) -> Optional[Network]:
        """Return the :class:`~yapapi.network.Network` to which this instance belongs (if any)"""
        return self.network_node.network if self.network_node else None

    @property
    def network_node(self) -> Optional[Node]:
        """Return the network :class:`~yapapi.network.Node` record associated with this instance."""
        return self._network_node

    def _set_cluster(self, cluster: "Cluster") -> None:
        self._cluster = cluster

    def _set_ctx(self, ctx: WorkContext) -> None:
        self._ctx = ctx

    def _set_network_node(self, node: Node) -> None:
        self._network_node = node

    def __repr__(self):
        class_name = type(self).__name__
        state = self.state.value
        provider_description = (
            f" on {self.provider_name} [ {self.provider_id} ]" if self.provider_id else ""
        )
        network_description = f" @ {self._network_node.ip}" if self._network_node else ""
        return f"<{class_name} {state}{provider_description}{network_description}>"

    def exc_info(self) -> ExcInfo:
        """Return exception info for an exception that caused the last state transition.

        If no such exception occurred, return `(None, None, None)`.
        """
        return self._exc_info

    async def send_message(self, message: Any = None):
        """Send a control message to this instance."""
        await self.__inqueue.put(ServiceSignal(message=message))

    def send_message_nowait(self, message: Optional[Any] = None):
        """Send a control message to this instance without blocking.

        May raise :class:`asyncio.QueueFull` if the channel for sending control messages is full.
        """
        self.__inqueue.put_nowait(ServiceSignal(message=message))

    async def receive_message(self) -> ServiceSignal:
        """Wait for a control message sent to this instance."""
        return await self.__outqueue.get()

    def receive_message_nowait(self) -> Optional[ServiceSignal]:
        """Retrieve a control message sent to this instance.

        Return `None` if no message is available.
        """
        try:
            return self.__outqueue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _listen(self) -> ServiceSignal:
        return await self.__inqueue.get()

    def _listen_nowait(self) -> Optional[ServiceSignal]:
        try:
            return self.__inqueue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _respond(self, message: Optional[Any], response_to: Optional[ServiceSignal] = None):
        await self.__outqueue.put(ServiceSignal(message=message, response_to=response_to))

    def _respond_nowait(self, message: Optional[Any], response_to: Optional[ServiceSignal] = None):
        self.__outqueue.put_nowait(ServiceSignal(message=message, response_to=response_to))

    @staticmethod
    async def get_payload() -> Optional[Payload]:
        """Return the payload (runtime) definition for this service.

        To be overridden by the author of a specific :class:`Service` class.

        If :func:`get_payload` is not implemented, the payload will need to be provided in the
        :func:`~yapapi.Golem.run_service` call.
        """
        pass

    def get_deploy_args(self) -> Dict:
        """Return the dictionary of kwargs needed to construct the `Deploy` exescript command."""
        kwargs = dict()
        if self._network_node:
            kwargs.update(self._network_node.get_deploy_args())
        return kwargs

    async def start(self) -> AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]]:
        """Implement the handler for the `starting` state of the service.

        To be overridden by the author of a specific :class:`Service` class.

        Should perform the minimum set of operations after which the instance of a service can be
        treated as "started", or, in other words, ready to receive service requests. It's up to the
        developer of the specific :class:`Service` class to decide what exact operations constitute a
        service startup. In the most common scenario :func:`~yapapi.script.Script.deploy()` and
        :func:`~yapapi.script.Script.start()` are required,
        check the `Default implementation` section for more details.

        As a handler implementing the `work generator pattern
        <https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals/hl-api-work-generator-pattern>`_,
        it's expected to be a generator that yields :class:`~yapapi.script.Script` (generated using the
        service's instance of the :class:`~yapapi.WorkContext` - :attr:`self._ctx`) that are then dispatched
        to the activity by the engine.

        Results of those batches can then be retrieved by awaiting the values captured from yield
        statements.

        A clean exit from a handler function triggers the engine to transition the state of the
        instance to the next stage in service's lifecycle - in this case, to `running`.

        On the other hand, any unhandled exception will cause the instance to be either retried on
        another provider node, if the :class:`Cluster`'s :attr:`respawn_unstarted_instances` argument is set to
        `True` in :func:`~yapapi.Golem.run_service`, which is also the default behavior, or altogether terminated, if
        :attr:`respawn_unstarted_instances` is set to `False`.

        **Example**::


            async def start(self):
                s = self._ctx.new_script()
                # deploy the exe-unit
                s.deploy(**self.get_deploy_args())
                # start the exe-unit's container
                s.start()
                # start some service process within the container
                s.run("/golem/run/service_ctl", "--start")
                # send the batch to the provider
                yield s

        ### Default implementation

        The default implementation assumes that, in order to accept commands, the runtime needs to
        be first deployed using the :func:`~yapapi.script.Script.deploy` command, which is analogous
        to creation of a container corresponding with the desired payload, and then started using the
        :func:`~yapapi.script.Script.start` command,
        actually launching the process that runs the aforementioned container.

        Additionally, it also assumes that the exe-unit doesn't need any additional parameters
        in its :func:`~yapapi.script.Script.start` call (e.g. for the VM runtime, all the required parameters are
        already passed as part of the agreement between the requestor and the provider), and parameters
        passed to :func:`~yapapi.script.Script.deploy` are returned by :func:`Service.get_deploy_args()` method.

        Therefore, this default implementation performs the minimum required for a VM payload to
        start responding to `run` commands. If your service requires any additional operations -
        you'll need to override this method (possibly first yielding from the parent - `super().start()` - generator)
        to add appropriate preparatory steps.

        In case of runtimes other than VM, `deploy` and/or `start` might be optional or altogether
        disallowed, or they may take some parameters. It is up to the author of the
        specific `Service` implementation that uses such a payload to adjust this method accordingly
        based on the requirements for the given runtime/exe-unit type.
        """
        assert self._ctx is not None

        s = self._ctx.new_script()
        s.deploy(**self.get_deploy_args())
        s.start()
        yield s

    async def run(self) -> AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]]:
        """Implement the handler for the `running` state of the service.

        To be overridden by the author of a specific :class:`Service` class.

        Should contain any operations needed to ensure continuous operation of a service.

        As a handler implementing the `work generator pattern
        <https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals/hl-api-work-generator-pattern>`_,
        it's expected to be a generator that yields :class:`~yapapi.script.Script` (generated using the
        service's instance of the :class:`~yapapi.WorkContext` - :attr:`self._ctx`) that are then dispatched
        to the activity by the engine.

        Results of those batches can then be retrieved by awaiting the values captured from `yield`
        statements.

        A clean exit from a handler function triggers the engine to transition the state of the
        instance to the next stage in service's lifecycle - in this case, to `stopping`.

        Any unhandled exception will cause the instance to be terminated.

        **Example**::

            async def run(self):
                while True:
                    script = self._ctx.new_script()
                    stats_results = script.run(self.SIMPLE_SERVICE, "--stats")
                    yield script
                    stats = (await stats_results).stdout.strip()
                    print(f"stats: {stats}")

        **Default implementation**

        Because the nature of the operations required during the "running" state depends directly
        on the specifics of a given :class:`Service` and because it's entirely plausible for a service
        not to require any direct interaction with the exe-unit (runtime) from the requestor's end
        after the service has been started, the default is to just wait indefinitely without
        producing any batches.
        """
        assert self._ctx is not None

        await asyncio.Future()
        yield  # type: ignore # unreachable because of the indefinite wait above

    async def shutdown(self) -> AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]]:
        """Implement the handler for the `stopping` state of the service.

        To be overridden by the author of a specific :class:`Service` class.

        Should contain any operations that the requestor needs to ensure the instance is correctly
        and gracefully shut-down - e.g. that its final state is retrieved.

        As a handler implementing the `work generator pattern
        <https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals/hl-api-work-generator-pattern>`_,
        it's expected to be a generator that yields :class:`~yapapi.script.Script` (generated using the
        service's instance of the :class:`~yapapi.WorkContext` - :attr:`self._ctx`) that are then dispatched
        to the activity by the engine.

        Results of those batches can then be retrieved by awaiting the values captured from yield
        statements.

        Finishing the execution of this handler will trigger termination of this instance.

        This handler will only be called if the activity running the service is still available.
        If the activity has already been deemed terminated or if the connection with the provider
        has been lost, the service will transition to the `terminated` state and the shutdown
        handler won't be run.

        **Example**::

            async def shutdown(self):
                self._ctx.run("/golem/run/dump_state")
                self._ctx.download_file("/golem/output/state", "/some/local/path/state")
                self._ctx.terminate()
                yield self._ctx.commit()

        **Default implementation**

        By default, the activity is just sent a `terminate` command. Whether it's absolutely
        required or not, again, depends on the implementation of the given runtime.

        """
        assert self._ctx is not None

        self._ctx.terminate()
        yield self._ctx.commit()

    async def reset(self) -> None:
        """Reset the service to the initial state.

        This method is called internally when the service is restarted in :class:`yapapi.services.ServiceRunner`,
        so it is not necessary for services that are never restarted (note that :func:`~yapapi.Golem.run_service()` by
        default restarts services that didn't start properly).

        Handlers of a restarted service are called more then once - all of the cleanup necessary between calls
        should be implemented here. E.g. if we initialize a counter in :func:`Service.__init__` and increment it
        in :func:`Service.start()`, we might want to reset it here to the initial value.

        Target implementation (0.10.0 and up) will raise NotImplementedError. Current implementation only warns about
        this future change.
        """

        msg = (
            "Default implementation of Service.reset will raise NotImplementedError starting from 0.10.0. "
            f"You should implement this method on {type(self)}"
        )
        warn_deprecated_msg(msg)

    @property
    def is_available(self):
        """Return `True` iff this instance is available (that is, starting, running or stopping)."""
        return self.__service_instance.state in ServiceState.AVAILABLE

    @property
    def state(self):
        """Return the current state of this instance."""
        return self.__service_instance.state

    @property
    def service_instance(self):
        return self.__service_instance


ServiceType = TypeVar("ServiceType", bound=Service)


@dataclass
class ServiceInstance:
    """ServiceRunner's service instance.

    A binding between the instance of the Service, its control queue and its state,
    used by the ServiceRunner to hold the complete state of each instance of a service.
    """

    service: Service
    control_queue: "asyncio.Queue[ControlSignal]" = field(default_factory=asyncio.Queue)
    service_state: ServiceState = field(default_factory=ServiceState)
    visited_states: List[statemachine.State] = field(default_factory=list)

    def __post_init__(self):
        self.service_state.instance = self

    @property
    def state(self) -> ServiceState:
        """Return the current state of this instance."""
        return self.service_state.current_state

    @property
    def started_successfully(self) -> bool:
        """Return `True` if this instance has entered `running` state, `False` otherwise."""
        return ServiceState.running in self.visited_states
