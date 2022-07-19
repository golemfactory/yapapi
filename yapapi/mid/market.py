from abc import ABC
import asyncio
from typing import AsyncIterator, Dict, Optional, TYPE_CHECKING

from ya_market import RequestorApi, models as ya_models

from .api_call_wrapper import api_call_wrapper
from .exceptions import ResourceNotFound
from .resource import Resource

if TYPE_CHECKING:
    from .golem_node import GolemNode


class MarketApiResource(Resource, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_market_api)


class Demand(MarketApiResource):
    @api_call_wrapper()
    async def _get_data(self) -> ya_models.Demand:
        #   NOTE: this method is required because there is no get_demand(id)
        #         in ya_market (as there is no matching endpoint in yagna)
        all_demands = await self.api.get_demands()
        try:
            return next(d for d in all_demands if d.demand_id == self.id)
        except StopIteration:
            raise ResourceNotFound('Demand', self.id)

    @classmethod
    async def create_from_properties_constraints(
        cls,
        node: "GolemNode",
        properties: Dict[str, str],
        constraints: str,
    ) -> "Demand":
        data = ya_models.DemandOfferBase(
            properties=properties,
            constraints=constraints,
        )
        return await cls.create(node, data)

    @classmethod
    async def create(cls, node: "GolemNode", data: ya_models.DemandOfferBase) -> "Demand":
        api = cls._get_api(node)
        demand_id = await api.subscribe_demand(data)
        return cls(node, demand_id)

    @api_call_wrapper(ignored_errors=[404, 410])
    async def unsubscribe(self) -> None:
        await self.api.unsubscribe_demand(self.id)

    async def offers(self) -> AsyncIterator["Offer"]:
        if self._event_collector is None:
            get_events = self.api.collect_offers
            self.start_collecting_events(get_events, self.id, timeout=5, max_events=10)

        assert self._event_collector is not None  # mypy
        queue: asyncio.Queue = self._event_collector.event_queue()

        while True:
            event = await queue.get()
            if isinstance(event, ya_models.ProposalEvent):
                offer = Offer.from_proposal_event(self.node, event)
                offer.demand = self
                yield offer

    def offer(self, offer_id: str) -> "Offer":
        offer = Offer(self.node, offer_id)
        offer.demand = self
        return offer


class Offer(MarketApiResource):
    _demand: Optional["Demand"] = None

    @property
    def demand(self) -> Optional["Demand"]:
        return self._demand

    @demand.setter
    def demand(self, demand: "Demand") -> None:
        self._demand = demand

    @api_call_wrapper()
    async def _get_data(self) -> ya_models.Offer:
        assert self.demand is not None
        return await self.api.get_proposal_offer(self.demand.id, self.id)

    @classmethod
    def from_proposal_event(cls, node: "GolemNode", event: ya_models.ProposalEvent) -> "Offer":
        data = event.proposal
        assert data.proposal_id is not None  # mypy
        return Offer(node, data.proposal_id, data)


class Agreement(MarketApiResource):
    pass
