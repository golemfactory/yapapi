from abc import ABC
import asyncio
from typing import AsyncIterator, TYPE_CHECKING

from ya_market import RequestorApi, models as ya_models

from .api_call_wrapper import api_call_wrapper
from .exceptions import ResourceNotFound
from .resource import Resource

if TYPE_CHECKING:
    from .golem_node import GolemNode


class PaymentApiResource(Resource, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_market_api)


class Demand(PaymentApiResource):
    async def _load_no_wrap(self):
        #   NOTE: this method is required because there is no get_demand(id)
        #         in ya_market (as there is no matching endpoint in yagna)
        all_demands = await self.api.get_demands()
        try:
            this_demands = [d for d in all_demands if d.demand_id == self.id]
            self._data = this_demands[0]
        except IndexError:
            raise ResourceNotFound('Demand', self.id)

    @classmethod
    async def create_from_properties_constraints(cls, node: "GolemNode", properties, constraints) -> "Demand":
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

    @api_call_wrapper
    async def unsubscribe(self) -> None:
        await self.api.unsubscribe_demand(self.id)

    async def offers(self) -> AsyncIterator["Offer"]:
        if self._event_collector is None:
            get_events = self.api.collect_offers
            self.start_collecting_events(get_events, self.id, timeout=5, max_events=10)

        queue: asyncio.Queue = self._event_collector.event_queue()

        while True:
            event = await queue.get()
            if isinstance(event, ya_models.ProposalEvent):
                yield Offer.from_proposal_event(self.node, event)


class Offer(PaymentApiResource):
    async def _load_no_wrap(self, demand_id):
        self._data = await self.api.get_proposal_offer(demand_id, self.id)

    @classmethod
    def from_proposal_event(cls, node: "GolemNode", event: ya_models.ProposalEvent) -> "Offer":
        data = event.proposal
        return Offer(node, data.proposal_id, data)


class Agreement(PaymentApiResource):
    pass
