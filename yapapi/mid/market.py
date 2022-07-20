from abc import ABC
import asyncio
from typing import AsyncIterator, Dict, Optional, TYPE_CHECKING, Union

from ya_market import RequestorApi, models as ya_models, exceptions

from .api_call_wrapper import api_call_wrapper
from .exceptions import ResourceNotFound
from .resource import Resource
from .yagna_event_collector import YagnaEventCollector

if TYPE_CHECKING:
    from .golem_node import GolemNode


class MarketApiResource(Resource, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_market_api)


class Demand(MarketApiResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        task = asyncio.get_event_loop().create_task(self._collect_offers())
        self._offer_collecting_task: Optional[asyncio.Task] = task

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
        await self.stop_collecting_events()
        await self.api.unsubscribe_demand(self.id)

    async def stop_collecting_events(self):
        if self._offer_collecting_task is not None:
            task = self._offer_collecting_task
            self._offer_collecting_task = None
            task.cancel()

    async def initial_offers(self) -> AsyncIterator["Offer"]:
        async for offer in self.child_aiter():
            if offer.initial:
                yield offer

    async def _collect_offers(self):
        event_collector = YagnaEventCollector(
            self.api.collect_offers,
            [self.id],
            {"timeout": 5, "max_events": 10},
        )
        event_collector.start()
        queue: asyncio.Queue = event_collector.event_queue()

        try:
            while True:
                event = await queue.get()
                if isinstance(event, ya_models.ProposalEvent):
                    offer = Offer.from_proposal_event(self.node, event)
                    parent = self._get_offer_parent(offer)
                    parent.add_child(offer)
        except asyncio.CancelledError:
            await event_collector.stop()

    def _get_offer_parent(self, offer: "Offer") -> Union["Demand", "Offer"]:
        if offer.initial:
            parent = self
        else:
            parent_offer_id = offer.data.prev_proposal_id
            parent = Offer(self.node, parent_offer_id)

            #   Sanity check - this should be true in all "expected" workflows,
            #   and we really want to detect any situation when it's not
            assert parent._parent is not None
        return parent

    def offer(self, offer_id: str) -> "Offer":
        offer = Offer(self.node, offer_id)

        #   NOTE: we don't know the parent, so we don't set it, but demand is known
        if offer._demand is None:
            offer.demand = self

        return offer


class Offer(MarketApiResource):
    _demand: Optional["Demand"] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    ##############################
    #   State-related properties
    @property
    def initial(self):
        assert self.data is not None
        return self.data.state == 'Initial'

    @property
    def draft(self):
        assert self.data is not None
        return self.data.state == 'Draft'

    ###########################
    #   Tree-related methods
    @property
    def demand(self) -> "Demand":
        assert self._demand is not None
        return self._demand

    @demand.setter
    def demand(self, demand: "Demand") -> None:
        assert self._demand is None
        self._demand = demand

    @property
    def parent(self) -> Union["Offer", "Demand"]:
        assert self._parent is not None
        return self._parent

    @parent.setter
    def parent(self, parent: Union["Offer", "Demand"]) -> None:
        assert self._parent is None
        self._parent = parent

        demand = parent if isinstance(parent, Demand) else parent.demand
        if self._demand is not None:
            assert self._demand is demand
        else:
            self.demand = demand

    async def responses(self) -> AsyncIterator["Offer"]:
        async for offer in self.child_aiter():
            yield offer

    ##########################
    #   Other
    @api_call_wrapper()
    async def _get_data(self) -> ya_models.Offer:
        assert self.demand is not None
        return await self.api.get_proposal_offer(self.demand.id, self.id)

    @classmethod
    def from_proposal_event(cls, node: "GolemNode", event: ya_models.ProposalEvent) -> "Offer":
        data = event.proposal
        assert data.proposal_id is not None  # mypy
        return Offer(node, data.proposal_id, data)

    async def respond(self) -> "Offer":
        data = await self._response_data()
        try:
            new_offer_id = await self.api.counter_proposal_demand(self.demand.id, self.id, data, _request_timeout=5)
        except exceptions.ApiException:
            raise

        new_offer = type(self)(self.node, new_offer_id)
        self.add_child(new_offer)

        return new_offer

    async def create_agreement(self) -> "Agreement":
        raise NotImplementedError
        # """Create an Agreement based on this Proposal."""
        # proposal = models.AgreementProposal(
        #     proposal_id=self.id,
        #     valid_to=datetime.now(timezone.utc) + timeout,
        # )
        # api: RequestorApi = self._subscription._api
        # agreement_id = await api.create_agreement(proposal)
        # return Agreement(api, self._subscription, agreement_id)

    async def _response_data(self) -> ya_models.DemandOfferBase:
        # FIXME: this is a mock
        demand_data = await self.demand.get_data()
        data = ya_models.DemandOfferBase(properties=demand_data.properties, constraints=demand_data.constraints)
        return data


class Agreement(MarketApiResource):
    pass
