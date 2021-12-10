"""Implementation of high-level services API."""
import itertools
from datetime import timedelta, datetime
import sys
from typing import (
    AsyncContextManager,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Type,
)

if sys.version_info >= (3, 8):
    from typing import Final
else:
    from typing_extensions import Final

from yapapi.network import Network
from yapapi.payload import Payload

from .service import Service
from .service_runner import ServiceRunner

# current defaults for yagna providers as of yagna 0.6.x, see
# https://github.com/golemfactory/yagna/blob/c37dbd1a2bc918a511eed12f2399eb9fd5bbf2a2/agent/provider/src/market/negotiator/factory.rs#L20
MIN_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=5)
MAX_AGREEMENT_EXPIRATION: Final[timedelta] = timedelta(minutes=180)

cluster_ids = itertools.count(1)


class Cluster(AsyncContextManager):
    """Golem's sub-engine used to spawn and control instances of a single :class:`Service`."""

    def __init__(
        self,
        service_runner: ServiceRunner,
        service_class: Type[Service],
        respawn_unstarted_instances: bool = True,
        network: Optional[Network] = None,
    ):
        """Initialize this Cluster.

        :param service_runner: ServiceRunner that will manage instances of this Cluster
        :param service_class: a subclass of :class:`~yapapi.services.Service` that represents the service to be run
        :param respawn_unstarted_instances: if an instance fails in the `starting` state,
            should this Cluster try to spawn another instance
        :param network: optional Network representing the VPN that this Cluster's instances will
            be attached to.
        """

        self.id = str(next(cluster_ids))

        self._service_runner = service_runner
        self._service_class = service_class
        self._task_ids = itertools.count(1)
        self._respawn_unstarted_instances = respawn_unstarted_instances
        self._network: Optional[Network] = network

    @property
    def expiration(self) -> datetime:
        """Return the expiration datetime for agreements related to services in this :class:`Cluster`."""
        return self._service_runner._job.expiration_time

    @property
    def payload(self) -> Payload:
        """Return the service runtime definition for this :class:`Cluster`."""
        return self._service_runner._job.payload

    @property
    def service_class(self) -> Type[Service]:
        """Return the class instantiated by all service instances in this :class:`Cluster`."""
        return self._service_class

    @property
    def network(self) -> Optional[Network]:
        """Return the :class:`~yapapi.network.Network` record associated with the VPN used by this :class:`Cluster`."""
        return self._network

    def __repr__(self):
        return (
            f"Cluster {self.id}: {len(self.__instances)}x[Service: {self._service_class.__name__}, "
            f"Payload: {self._payload}]"
        )

    @property
    def instances(self) -> List[Service]:
        """Return the list of service instances in this :class:`Cluster`."""
        return self._service_runner.instances.copy()

    def stop_instance(self, service: Service):
        """Stop the specific :class:`Service` instance belonging to this :class:`Cluster`."""
        self._service_runner.stop_instance(service)

    def spawn_instances(
        self,
        num_instances: Optional[int] = None,
        instance_params: Optional[Iterable[Dict]] = None,
        network_addresses: Optional[List[str]] = None,
    ) -> None:
        """Spawn new instances within this :class:`Cluster`.
        :param num_instances: optional number of service instances to run. Defaults to a single
            instance, unless `instance_params` is given, in which case, the :class:`Cluster` will spawn
            as many instances as there are elements in the `instance_params` iterable.
            if `num_instances` is not None and < 1, the method will immediately return and log a warning.
        :param instance_params: optional list of dictionaries of keyword arguments that will be passed
            to consecutive, spawned instances. The number of elements in the iterable determines the
            number of instances spawned, unless `num_instances` is given, in which case the latter takes
            precedence.
            In other words, if both `num_instances` and `instance_params` are provided,
            the number of instances spawned will be equal to `num_instances` and if there are
            too few elements in the `instance_params` iterable, it will results in an error.
        :param network_addresses: optional list of network addresses in case the :class:`Cluster` is
            attached to VPN. If the list is not provided (or if the number of elements is less
            than the number of spawned instances), any instances for which the addresses have not
            been given, will be assigned an address automatically.
        """

        instance_param_gen = self._resolve_instance_params(num_instances, instance_params)
        for ix, single_instance_params in enumerate(instance_param_gen):
            network_address = None
            if network_addresses is not None and len(network_addresses) > ix:
                network_address = network_addresses[ix]

            service = self.service_class(**single_instance_params)  # type: ignore
            self.service_runner.add_instance(service, network_address)

    def stop(self):
        """Signal the whole :class:`Cluster` and the underlying ServiceRunner to stop."""
        self._service_runner.stop()

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
