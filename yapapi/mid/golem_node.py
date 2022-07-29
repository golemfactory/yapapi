import asyncio
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterable, Optional, List, Set, Type, TypeVar, Union
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from yapapi import rest
from yapapi.engine import DEFAULT_DRIVER, DEFAULT_NETWORK, DEFAULT_SUBNET
from yapapi.payload import Payload
from yapapi.props.builder import DemandBuilder
from yapapi import props

from .event_bus import EventBus
from .payment import Allocation
from .market import Demand, Proposal, Agreement
from .resource import Resource


DEFAULT_EXPIRATION_TIMEOUT = timedelta(seconds=1800)
ResourceType = TypeVar("ResourceType", bound=Resource)


class GolemNode:
    def __init__(self, app_key: str = None, base_url: str = None) -> None:
        """Main entrypoint to the python Golem API, communicates with `yagna`.

        GolemNode object corresponds to a single running `yagna` instance
        and can operate on different subnets / payment networks.

        :param app_key: App key used as an authentication token for all `yagna` calls.
                        Defaults to the `YAGNA_APPKEY` env variable.
        :param base_url: Base url for all `yagna` APIs. Defaults to `YAGNA_API_URL` env
                         variable or http://127.0.0.1:7465.

        Usage::

            golem = GolemNode()
            async with golem:
                #   Interact with the Golem Network

        """
        self._api_config = rest.Configuration(app_key, url=base_url)

        #   All created Resources will be stored here
        #   (This is done internally by the metaclass of the Resource)
        self._resources: DefaultDict[Type[Resource], Dict[str, Resource]] = defaultdict(dict)
        self._autoclose_resources: Set[Resource] = set()
        self._event_bus: EventBus = EventBus()

    ########################
    #   Start/stop interface
    async def __aenter__(self) -> "GolemNode":
        """Start. Initialize all the APIs and the event bus."""
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        """Shutdown. Stop collecting yagna events, close all resources created with autoclose=True, close APIs etc."""
        await self.aclose()

    async def start(self) -> None:
        self._event_bus.start()
        self._ya_market_api = self._api_config.market()
        self._ya_activity_api = self._api_config.activity()
        self._ya_payment_api = self._api_config.payment()
        self._ya_net_api = self._api_config.net()

    async def aclose(self) -> None:
        self._set_no_more_children()
        await self._stop_event_collectors()
        await self._close_autoclose_resources()
        await self._close_apis()
        await self._event_bus.stop()

    async def _stop_event_collectors(self) -> None:
        #   NOTE: now only Demands collect events, but this will change in the future
        demands = self._all_resources(Demand)
        tasks = [demand.stop_collecting_events() for demand in demands]
        if tasks:
            await asyncio.gather(*tasks)

    def _set_no_more_children(self) -> None:
        for resources in self._resources.values():
            for resource in resources.values():
                resource.set_no_more_children()

    async def _close_apis(self) -> None:
        await asyncio.gather(
            self._ya_market_api.close(),
            self._ya_activity_api.close(),
            self._ya_payment_api.close(),
            self._ya_net_api.close(),
        )

    async def _close_autoclose_resources(self) -> None:
        agreement_msg = "Work finished"
        agreement_tasks = [r.terminate(agreement_msg) for r in self._autoclose_resources if isinstance(r, Agreement)]
        demand_tasks = [r.unsubscribe() for r in self._autoclose_resources if isinstance(r, Demand)]
        allocation_tasks = [r.release() for r in self._autoclose_resources if isinstance(r, Allocation)]
        if agreement_tasks:
            await asyncio.gather(*agreement_tasks)
        if demand_tasks:
            await asyncio.gather(*demand_tasks)
        if allocation_tasks:
            await asyncio.gather(*allocation_tasks)

    ###########################
    #   Create new resources
    async def create_allocation(
        self,
        amount: Union[Decimal, float],
        network: str = DEFAULT_NETWORK,
        driver: str = DEFAULT_DRIVER,
        autoclose: bool = True,
    ) -> Allocation:
        """Create a new allocation.

        :param amount: Amount of GLMs to be allocated
        :param network: Payment network
        :param driver: Payment driver
        :param autoclose: Release allocation on :func:`__aexit__`
        """
        decimal_amount = Decimal(amount)

        #   TODO (?): It is assumed we have only a single account for (network, driver).
        #             In the future this assumption might not be true, but we don't care now.
        allocation = await Allocation.create_any_account(self, decimal_amount, network, driver)
        if autoclose:
            self.add_autoclose_resource(allocation)
        return allocation

    async def create_demand(
        self,
        payload: Payload,
        subnet: Optional[str] = DEFAULT_SUBNET,
        expiration: Optional[datetime] = None,
        allocations: Iterable[Allocation] = (),
        autoclose: bool = True,
        autostart: bool = True,
    ) -> Demand:
        """Subscribe a new demand.

        :param payload: Details of the demand
        :param subnet: Subnet tag
        :param expiration: Timestamp when all agreements based on this demand will expire (TODO: is this correct?)
        :param allocations: Allocations that will be included in the description of this demand.
        :param autoclose: Unsubscribe demand on :func:`__aexit__`
        :param autostart: Immediately start collecting yagna events for this :class:`~yapapi.mid.market.Demand`.
                          Without autostart events for this demand will start being collected after a call to
                          :func:`~yapapi.mid.market.Demand.start_collecting_events`.
        """
        if expiration is None:
            expiration = datetime.now(timezone.utc) + DEFAULT_EXPIRATION_TIMEOUT

        builder = DemandBuilder()
        builder.add(props.Activity(expiration=expiration, multi_activity=True))
        builder.add(props.NodeInfo(subnet_tag=subnet))

        await builder.decorate(payload)
        await self._add_builder_allocations(builder, allocations)

        demand = await Demand.create_from_properties_constraints(self, builder.properties, builder.constraints)

        if autostart:
            demand.start_collecting_events()
        if autoclose:
            self.add_autoclose_resource(demand)
        return demand

    async def _add_builder_allocations(self, builder: DemandBuilder, allocations: Iterable[Allocation]) -> None:
        for allocation in allocations:
            properties, constraints = await allocation.demand_properties_constraints()
            for constraint in constraints:
                builder.ensure(constraint)

            #   TODO (?): It is assumed there are no conflicts here (i.e. allocations for different addresses
            #             for the same network/driver pair). One day we might need to change this.
            builder.properties.update({p.key: p.value for p in properties})

    ###########################
    #   Single-resource factories for already existing resources
    def allocation(self, allocation_id: str) -> Allocation:
        """Returns an :any:`Allocation` with a given id (assumed to be correct, there is no validation)."""
        return Allocation(self, allocation_id)

    def demand(self, demand_id: str) -> Demand:
        """Returns a :any:`Demand` with a given id (assumed to be correct, there is no validation)."""
        return Demand(self, demand_id)

    def proposal(self, proposal_id: str, demand_id: str) -> Proposal:
        """Returns a :any:`Proposal` with a given id (assumed to be correct, there is no validation).

        Id of a proposal has a meaning only in the context of a demand,
        so demand_id is also necessary (and also not validated)."""
        demand = self.demand(demand_id)
        return demand.proposal(proposal_id)

    def agreement(self, agreement_id: str) -> Agreement:
        """Returns an :any:`Agreement` with a given id (assumed to be correct, there is no validation)."""
        return Agreement(self, agreement_id)

    ##########################
    #   Multi-resource factories for already existing resources
    async def allocations(self) -> List[Allocation]:
        """Returns a list of :any:`Allocation` objects corresponding to all current allocations.

        These are all allocations related to the current APP_KEY - it doesn't matter if they were created
        with this :class:`GolemNode` instance (or if :class:`GolemNode` was used at all).
        """
        return await Allocation.get_all(self)

    async def demands(self) -> List[Demand]:
        """Returns a list of :any:`Demand` objects corresponding to all current demands.

        These are all demands subscribed with the current APP_KEY - it doesn't matter if they were created
        with this :class:`GolemNode` instance (or if :class:`GolemNode` was used at all).
        """
        return await Demand.get_all(self)

    ##########################
    #   Events
    @property
    def event_bus(self) -> EventBus:
        """Returns the :class:`EventBus` used by this :class:`GolemNode`.

        Events caused by this :class:`GolemNode` and objects operating on it are emitted on this event bus.
        """
        return self._event_bus

    #########
    #   Other
    def add_autoclose_resource(self, resource: Union["Allocation", "Demand", "Agreement"]) -> None:
        self._autoclose_resources.add(resource)

    def _all_resources(self, cls: Type[ResourceType]) -> List[ResourceType]:
        return list(self._resources[cls].values())  # type: ignore

    def __str__(self) -> str:
        lines = [
            f"{type(self).__name__}(",
            f"  app_key = {self._api_config.app_key},",
            f"  market_url = {self._api_config.market_url},",
            f"  payment_url = {self._api_config.payment_url},",
            f"  activity_url = {self._api_config.activity_url},",
            f"  net_url = {self._api_config.net_url},",
            f")",
        ]
        return "\n".join(lines)
