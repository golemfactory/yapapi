from abc import ABC
import asyncio
from typing import AsyncIterator, Dict, List, Optional, TYPE_CHECKING, Union

from ya_market import RequestorApi, models as ya_models, exceptions

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
                await self._set_offer_parent(offer)
                yield offer

    def offer(self, offer_id: str) -> "Offer":
        offer = Offer(self.node, offer_id)
        offer.demand = self
        return offer

    async def _set_offer_parent(self, offer: "Offer") -> None:
        if offer.initial:
            parent = self
        else:
            parent_offer_id = offer.data.prev_proposal_id
            parent = Offer(self.node, parent_offer_id)
            assert parent._parent is not None
        parent.add_child(offer)

    def add_child(self, offer: "Offer") -> None:
        #   TODO
        offer.parent = self


class Offer(MarketApiResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent: Optional[Union["Demand", "Offer"]] = None
        self._child_offers: List["Offer"] = []

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
    def parent(self) -> Union["Demand", "Offer"]:
        assert self._parent is not None
        return self._parent

    @parent.setter
    def parent(self, parent: Union["Demand", "Offer"]) -> None:
        assert self._parent is None
        self._parent = parent

    @property
    def demand(self) -> "Demand":
        return self.parent if isinstance(self.parent, Demand) else self.parent.demand

    def add_child(self, offer: "Offer") -> None:
        offer.parent = self
        self._child_offers.append(offer)

    async def offers(self) -> AsyncIterator["Offer"]:
        cnt = 0
        while True:
            if cnt < len(self._child_offers):
                yield self._child_offers[cnt]
                cnt += 1
            else:
                await asyncio.sleep(0.1)

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
        assert self.demand is not None

        data = await self._response_data()
        try:
            new_offer_id = await self.api.counter_proposal_demand(self.demand.id, self.id, data, _request_timeout=5)
        except exceptions.ApiException:
            raise  # TODO what is going on with this missing subscription?

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
        demand_data = await self.demand.load()
        data = ya_models.DemandOfferBase(properties=demand_data.properties, constraints=demand_data.constraints)
        return data


class Agreement(MarketApiResource):
    pass
