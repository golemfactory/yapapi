"""Implementation of high-level services API."""
import itertools
from datetime import timedelta, datetime, timezone
import sys
from typing import (
    AsyncContextManager,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    Optional,
    Type,
)

if sys.version_info >= (3, 8):
    from typing import Final
    from contextlib import AsyncExitStack
else:
    from typing_extensions import Final
    from async_exit_stack import AsyncExitStack  # type: ignore

from yapapi.network import Network
from yapapi.payload import Payload
from yapapi.engine import _Engine, Job

from .service import ServiceType
from .service_runner import ServiceRunner

DEFAULT_SERVICE_EXPIRATION: Final[timedelta] = timedelta(minutes=180)


class Cluster(AsyncContextManager, Generic[ServiceType]):
    """Golem's sub-engine used to spawn and control instances of a single :class:`Service`."""

    def __init__(
        self,
        engine: _Engine,
        service_class: Type[ServiceType],
        payload: Payload,
        expiration: Optional[datetime] = None,
        respawn_unstarted_instances: bool = True,
        network: Optional[Network] = None,
    ):
        """Initialize this Cluster.

        :param engine: an engine for running service instance
        :param service_class: a subclass of :class:`~yapapi.services.Service` that represents the service to be run
        :param payload: definition of service runtime for this Cluster
        :param expiration: a date before which all agreements related to running services
            in this Cluster should be terminated
        :param respawn_unstarted_instances: if an instance fails in the `starting` state,
            should this Cluster try to spawn another instance
        :param network: optional Network representing the VPN that this Cluster's instances will
            be attached to.
        """
        expiration = expiration or self._default_expiration()

        job = Job(engine, expiration, payload)
        self.service_runner = ServiceRunner(job)
        self._service_class = service_class
        self._respawn_unstarted_instances = respawn_unstarted_instances
        self._network: Optional[Network] = network

        self._task_ids = itertools.count(1)
        self._stack = AsyncExitStack()

    async def __aenter__(self):
        await self._stack.enter_async_context(self.service_runner)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._terminate(exc_type, exc_val, exc_tb)

    async def terminate(self):
        """Signal the whole :class:`Cluster` and the underlying :class:`~yapapi.service.ServiceRunner` to stop."""
        await self._terminate(None, None, None)

    def stop(self):
        """Stop all services in this :class:`Cluster`"""
        for instance in self.instances:
            self.stop_instance(instance)

    async def _terminate(self, exc_type, exc_val, exc_tb):
        #   NOTE: this might be called more then once (e.g. by `terminate()` followed by `__aexit__`),
        #   but it's harmless, so we don't care
        self.stop()
        await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def id(self) -> str:
        return self.service_runner.id

    @property
    def expiration(self) -> datetime:
        """Return the expiration datetime for agreements related to services in this :class:`Cluster`."""
        return self.service_runner._job.expiration_time

    @property
    def payload(self) -> Payload:
        """Return the service runtime definition for this :class:`Cluster`."""
        return self.service_runner._job.payload

    @property
    def service_class(self) -> Type[ServiceType]:
        """Return the class instantiated by all service instances in this :class:`Cluster`."""
        return self._service_class

    @property
    def network(self) -> Optional[Network]:
        """Return the :class:`~yapapi.network.Network` record associated with the VPN used by this :class:`Cluster`."""
        return self._network

    def __repr__(self):
        return (
            f"Cluster {self.id}: {len(self.instances)}x[Service: {self._service_class.__name__}, "
            f"Payload: {self.payload}]"
        )

    @property
    def instances(self) -> List[ServiceType]:
        """Return the list of service instances in this :class:`Cluster`."""
        return self.service_runner.instances.copy()

    def stop_instance(self, service: ServiceType):
        """Stop the specific :class:`Service` instance belonging to this :class:`Cluster`."""
        self.service_runner.stop_instance(service)

    def spawn_instances(
        self,
        num_instances: Optional[int] = None,
        instance_params: Optional[Iterable[Dict]] = None,
        network_addresses: Optional[List[str]] = None,
    ) -> None:
        """Spawn new instances within this :class:`Cluster`.

        :param num_instances: optional number of service instances to run. Defaults to
            a single instance, unless `instance_params` is given, in which case,
            the :class:`Cluster` will spawn as many instances as there are elements in
            the `instance_params` iterable.
            if `num_instances` is not None and < 1, the method will immediately return
            and log a warning.
        :param instance_params: optional list of dictionaries of keyword arguments that
            will be passed to the `__init__` of the consecutive, spawned instances.
            The number of elements in the iterable determines the number of instances
            spawned, unless `num_instances` is given, in which case the latter takes
            precedence.
            In other words, if both `num_instances` and `instance_params` are provided,
            the number of instances spawned will be equal to `num_instances` and if
            there are too few elements in the `instance_params` iterable, it will
            result in an error.
        :param network_addresses: optional list of network addresses in case the
            :class:`Cluster` is attached to VPN. If the list is not provided
            (or if there are fewer elements than the number of spawned instances), any
            instances for which the addresses have not been given, will be assigned an
            address automatically.
        """

        instance_param_gen = self._resolve_instance_params(num_instances, instance_params)
        for ix, single_instance_params in enumerate(instance_param_gen):
            network_address = None
            if network_addresses is not None and len(network_addresses) > ix:
                network_address = network_addresses[ix]

            service = self.service_class(**single_instance_params)  # type: ignore
            respawn_condition = (
                self._instance_not_started if self._respawn_unstarted_instances else None
            )
            self.service_runner.add_instance(
                service, self.network, network_address, respawn_condition
            )
            service._set_cluster(self)

    @staticmethod
    def _instance_not_started(service: ServiceType) -> bool:
        return (
            service.exc_info() != (None, None, None)
            and not service.service_instance.started_successfully
        )

    def _resolve_instance_params(
        self,
        num_instances: Optional[int],
        instance_params: Optional[Iterable[Dict]],
    ) -> Generator[Dict, None, None]:
        if instance_params is None:
            if num_instances is None:
                num_instances = 1
            yield from ({} for _ in range(num_instances))
        else:
            instance_params = iter(instance_params)
            if num_instances is None:
                yield from instance_params
            else:
                for i in range(num_instances):
                    try:
                        yield next(instance_params)
                    except StopIteration:
                        raise ValueError(
                            f"`instance_params` iterable depleted after {i} spawned instances."
                        )

    def _default_expiration(self):
        return datetime.now(timezone.utc) + DEFAULT_SERVICE_EXPIRATION
