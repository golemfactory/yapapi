import asyncio
from collections import defaultdict
from typing import DefaultDict, Dict, Iterable, Optional, Tuple, Type
from datetime import datetime, timedelta, timezone

from yapapi import rest
from yapapi.engine import DEFAULT_DRIVER, DEFAULT_NETWORK, DEFAULT_SUBNET
from yapapi.payload import Payload
from yapapi.props.builder import DemandBuilder
from yapapi import props

from .event_bus import EventBus
from .payment import Allocation
from .market import Demand, Offer, Agreement
from .resource import Resource


DEFAULT_EXPIRATION_TIMEOUT = timedelta(seconds=1800)


class GolemNode:
    def __init__(self):
        self._api_config = rest.Configuration()

        #   All created Resources will be stored here
        #   (This is done internally by the metaclass of the Resource)
        self._resources: DefaultDict[Type[Resource], Dict[str, Resource]] = defaultdict(dict)
        self._event_bus = EventBus()

    ########################
    #   Start/stop interface
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc_info):
        await self.aclose()

    async def start(self):
        self._event_bus.start()
        self._ya_market_api = self._api_config.market()
        self._ya_activity_api = self._api_config.activity()
        self._ya_payment_api = self._api_config.payment()
        self._ya_net_api = self._api_config.net()

    async def aclose(self):
        await self._stop_collecting_events()
        await self._close_apis()
        await self._event_bus.stop()

    async def _stop_collecting_events(self):
        tasks = []
        for resources in self._resources.values():
            tasks += [obj.stop_collecting_events() for obj in resources.values()]
        await asyncio.gather(*tasks)

    async def _close_apis(self):
        await asyncio.gather(
            self._ya_market_api.close(),
            self._ya_activity_api.close(),
            self._ya_payment_api.close(),
            self._ya_net_api.close(),
        )

    ###########################
    #   Create new resources
    async def create_allocation(
        self,
        amount: float,
        network: str = DEFAULT_NETWORK,
        driver: str = DEFAULT_DRIVER,
    ) -> Allocation:
        #   TODO: This creates an Allocation for a matching requestor account
        #         (or raises an exception if there is none).
        #         Can we have more than a single matching account?
        #         If yes - how to approach this? Add `create_allocations`?
        return await Allocation.create_any_account(self, amount, network, driver)

    async def create_demand(
        self,
        payload: Payload,
        subnet: str = DEFAULT_SUBNET,
        expiration: Optional[datetime] = None,
        allocations: Iterable[Allocation] = (),
    ) -> Demand:
        if expiration is None:
            expiration = datetime.now(timezone.utc) + DEFAULT_EXPIRATION_TIMEOUT

        builder = DemandBuilder()
        builder.add(props.Activity(expiration=expiration, multi_activity=True))
        builder.add(props.NodeInfo(subnet_tag=subnet))

        await builder.decorate(payload)
        await self._add_builder_allocations(builder, allocations)
        return await Demand.create_from_properties_constraints(self, builder.properties, builder.constraints)

    async def _add_builder_allocations(self, builder: DemandBuilder, allocations: Iterable[Allocation]) -> None:
        for allocation in allocations:
            properties, constraints = await allocation.demand_properties_constraints()
            for constraint in constraints:
                builder.ensure(constraint)

            #   TODO: what if we already have such properties in builder?
            #         E.g. we have different address in allocations?
            #         (Now this should work just as in `yapapi.Engine`, but there we
            #         can't have different addresses I guess?)
            builder.properties.update({p.key: p.value for p in properties})

    ###########################
    #   Single-resource factories for already existing resources
    def allocation(self, allocation_id) -> Allocation:
        return Allocation(self, allocation_id)

    def demand(self, demand_id) -> Demand:
        return Demand(self, demand_id)

    def offer(self, offer_id) -> Offer:
        return Offer(self, offer_id)

    def agreement(self, agreement_id) -> Agreement:
        return Agreement(self, agreement_id)

    ##########################
    #   Multi-resource factories for already existing resources
    async def allocations(self) -> Tuple[Allocation]:
        return await Allocation.get_all(self)

    async def demands(self) -> Tuple[Demand]:
        return await Demand.get_all(self)

    ##########################
    #   Events
    #   (TODO: do we want the whole EventBus API repeated here? "listen" etc?)
    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    #########
    #   Other
    def __str__(self):
        lines = [
            f"{type(self).__name__}(",
            f"  app_key = {self._api_config.app_key},",
            f"  market_url = {self._api_config.market_url},",
            f"  payment_url = {self._api_config.payment_url},",
            f"  activity_url = {self._api_config.activity_url},",
            f"  net_url = {self._api_config.net_url},",
            f"  gsb_url = TODO? this is used by yagna only, but is part of the config?",
            f")",
        ]
        return "\n".join(lines)
