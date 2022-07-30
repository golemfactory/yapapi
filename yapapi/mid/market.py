import asyncio
from typing import AsyncIterator, Dict, List, Optional, TYPE_CHECKING, Union
from datetime import datetime, timedelta, timezone

from ya_market import RequestorApi, models as models, exceptions

from .api_call_wrapper import api_call_wrapper
from .exceptions import ResourceNotFound
from .resource import Resource
from .resource_internals import _NULL
from .yagna_event_collector import YagnaEventCollector
from .events import ResourceClosed

if TYPE_CHECKING:
    from .golem_node import GolemNode


class Demand(Resource[RequestorApi, models.Demand, _NULL, "Proposal", _NULL]):
    """A single demand on the Golem Network.

    Created with one of the :class:`Demand`-returning methods of the :class:`~yapapi.mid.golem_node.GolemNode`.
    """
    _event_collecting_task: Optional[asyncio.Task] = None

    ######################
    #   EXTERNAL INTERFACE
    def start_collecting_events(self) -> None:
        """Start collecting `yagna` events in response to this demand.

        Each event is either a new initial :class:`Proposal` (yielded in :func:`initial_proposals`),
        a response to our counter-proposal (accessible via :func:`Proposal.responses`),
        or a rejection of a proposal.
        """
        if self._event_collecting_task is None:
            task = asyncio.get_event_loop().create_task(self._process_yagna_events())
            self._event_collecting_task = task

    async def stop_collecting_events(self) -> None:
        """Stop collecting events, after a prior call to :func:`start_collecting_events`."""
        if self._event_collecting_task is not None:
            self._event_collecting_task.cancel()
            self._event_collecting_task = None

    @api_call_wrapper(ignore=[404, 410])
    async def unsubscribe(self) -> None:
        """Stop all operations related to this demand and remove it.

        This is a final operation, unsubscribed demand is not available anymore."""
        self.set_no_more_children()
        await self.stop_collecting_events()
        await self.api.unsubscribe_demand(self.id)
        self.node.event_bus.emit(ResourceClosed(self))

    async def initial_proposals(self) -> AsyncIterator["Proposal"]:
        """Yields initial proposals matched to this demand."""
        async for proposal in self.child_aiter():
            assert isinstance(proposal, Proposal)  # mypy
            if proposal.initial:
                yield proposal

    def proposal(self, proposal_id: str) -> "Proposal":
        """Return a :class:`Proposal` with a given ID."""
        proposal = Proposal(self.node, proposal_id)

        #   NOTE: we don't know the parent, so we don't set it, but demand is known
        if proposal._demand is None and proposal._parent is None:
            proposal.demand = self

        return proposal

    #################
    #   OTHER METHODS
    @api_call_wrapper()
    async def _get_data(self) -> models.Demand:
        #   NOTE: this method is required because there is no get_demand(id)
        #         in ya_market (as there is no matching endpoint in yagna)
        all_demands: List[models.Demand] = await self.api.get_demands()
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
        data = models.DemandOfferBase(
            properties=properties,
            constraints=constraints,
        )
        return await cls.create(node, data)

    @classmethod
    async def create(cls, node: "GolemNode", data: models.DemandOfferBase) -> "Demand":
        api = cls._get_api(node)
        demand_id = await api.subscribe_demand(data)
        return cls(node, demand_id)

    async def _process_yagna_events(self) -> None:
        event_collector = YagnaEventCollector(
            self.api.collect_offers,
            [self.id],
            {"timeout": 5, "max_events": 10},
        )
        async with event_collector:
            queue: asyncio.Queue = event_collector.event_queue()
            while True:
                event = await queue.get()
                if isinstance(event, models.ProposalEvent):
                    proposal = Proposal.from_proposal_event(self.node, event)
                    parent = self._get_proposal_parent(proposal)
                    parent.add_child(proposal)
                elif isinstance(event, models.ProposalRejectedEvent):
                    assert event.proposal_id is not None  # mypy
                    proposal = self.proposal(event.proposal_id)
                    proposal.add_event(event)

    def _get_proposal_parent(self, proposal: "Proposal") -> Union["Demand", "Proposal"]:
        if proposal.initial:
            parent = self
        else:
            parent_proposal_id = proposal.data.prev_proposal_id
            parent = Proposal(self.node, parent_proposal_id)  # type: ignore

            #   Sanity check - this should be true in all "expected" workflows,
            #   and we really want to detect any situation when it's not
            assert parent._parent is not None
        return parent


class Proposal(
    Resource[
        RequestorApi,
        models.Proposal,
        Union["Demand", "Proposal"],
        Union["Proposal", "Agreement"],
        Union[models.ProposalEvent, models.ProposalRejectedEvent]
    ]
):
    """A single proposal on the Golem Network.

    Either a initial proposal matched to a demand, or a counter-proposal sent
    either by us or by the provider.

    Sample usage::

        initial_proposal = await demand.initial_proposals().__anext__()
        our_counter_proposal = await initial_proposal.respond()
        async for their_counter_proposal in our_counter_proposal.responses():
            agreement = their_counter_proposal.create_agreement()
            break
        else:
            print("Our counter-proposal was rejected :(")
    """
    _demand: Optional["Demand"] = None

    ##############################
    #   State-related properties
    @property
    def initial(self) -> bool:
        """True for proposals matched directly to the demand."""
        assert self.data is not None
        return self.data.state == 'Initial'

    @property
    def draft(self) -> bool:
        """True for proposals that are responses to other proposals."""
        assert self.data is not None
        return self.data.state == 'Draft'

    @property
    def rejected(self) -> bool:
        """True for rejected proposals. They will have no more :func:`responses`."""
        assert self.data is not None
        return self.data.state == 'Rejected'

    ###########################
    #   Tree-related methods
    @property
    def demand(self) -> "Demand":
        """Initial :class:`Demand` of this proposal."""
        # We can either have no parent (this is possible when this Proposal was created from id),
        # and then _demand is always set, or a Proposal-parent or a Demand-parent.
        if self._demand is not None:
            return self._demand
        else:
            if isinstance(self.parent, Demand):
                return self.parent
            else:
                return self.parent.demand

    @demand.setter
    def demand(self, demand: "Demand") -> None:
        assert self._demand is None
        assert self._parent is None  # Sanity check (there's no scenario where we have a parent and demand is set)
        self._demand = demand

    def add_event(self, event: Union[models.ProposalEvent, models.ProposalRejectedEvent]) -> None:
        super().add_event(event)
        if isinstance(event, models.ProposalRejectedEvent):
            self.set_no_more_children()

    async def responses(self) -> AsyncIterator["Proposal"]:
        """Yields responses to this proposal.

        Stops when the proposal is rejected.
        """
        async for child in self.child_aiter():
            if isinstance(child, Proposal):
                yield child

    ############################
    #   Negotiations
    @api_call_wrapper()
    async def create_agreement(self, autoclose: bool = True, timeout: timedelta = timedelta(seconds=60)) -> "Agreement":
        """Promote this proposal to an agreement.

        :param autoclose: Terminate the agreement when the :class:`~yapapi.mid.golem_node.GolemNode` closes.
        :param timeout: TODO - this is used as `AgreementValidTo`, but what is it exactly?
        """
        proposal = models.AgreementProposal(
            proposal_id=self.id,
            valid_to=datetime.now(timezone.utc) + timeout,  # type: ignore  # TODO: what is AgreementValidTo?
        )
        agreement_id = await self.api.create_agreement(proposal)
        agreement = Agreement(self.node, agreement_id)
        self.add_child(agreement)
        if autoclose:
            self.node.add_autoclose_resource(agreement)

        return agreement

    @api_call_wrapper()
    async def reject(self, reason: str = '') -> None:
        """Reject the proposal - inform the provider that we won't send any more counter-proposals.

        :param reason: An optional information for the provider describing rejection reasons.

        Invalid on our responses.
        """
        await self.api.reject_proposal_offer(
            self.demand.id, self.id, request_body={"message": reason}, _request_timeout=5
        )

    @api_call_wrapper()
    async def respond(self) -> "Proposal":
        """Respond to a proposal with a counter-proposal.

        Invalid on proposals sent by the provider.

        TODO: all the negotiation logic should be reflected in params of this method,
        but negotiations are not implemented yet.
        """

        data = await self._response_data()
        new_proposal_id = await self.api.counter_proposal_demand(self.demand.id, self.id, data, _request_timeout=5)

        new_proposal = type(self)(self.node, new_proposal_id)
        self.add_child(new_proposal)

        return new_proposal

    async def _response_data(self) -> models.DemandOfferBase:
        # FIXME: this is a mock
        demand_data = await self.demand.get_data()
        data = models.DemandOfferBase(properties=demand_data.properties, constraints=demand_data.constraints)
        return data

    ##########################
    #   Other
    async def _get_data(self) -> models.Proposal:
        assert self.demand is not None
        data: models.Proposal = await self.api.get_proposal_offer(self.demand.id, self.id)
        if data.state == "Rejected":
            self.set_no_more_children()
        return data

    @classmethod
    def from_proposal_event(cls, node: "GolemNode", event: models.ProposalEvent) -> "Proposal":
        data = event.proposal
        assert data.proposal_id is not None  # mypy
        proposal = Proposal(node, data.proposal_id, data)
        proposal.add_event(event)
        return proposal


class Agreement(Resource[RequestorApi, models.Agreement, "Proposal", _NULL, _NULL]):
    """A single agreement on the Golem Network.

    Sample usage::

        agreement = await proposal.create_agreement()
        await agreement.confirm()
        await agreement.wait_for_approval()
        #   Create activity, use the activity
        await agreement.terminate()
    """
    @api_call_wrapper()
    async def confirm(self) -> None:
        """Confirm the agreement.

        First step that leads to an active agreement.
        """
        await self.api.confirm_agreement(self.id)

    @api_call_wrapper()
    async def wait_for_approval(self) -> bool:
        """Wait for provider's approval of the agreement.

        Second (and last) step leading to an active agreement.

        :returns: True if agreement was approved.
        """
        try:
            await self.api.wait_for_approval(self.id, timeout=15, _request_timeout=16)
            return True
        except exceptions.ApiException as e:
            if e.status == 410:
                return False
            elif e.status == 408:
                #   TODO: maybe this should be in api_call_wrapper?
                return await self.wait_for_approval()
            else:
                raise

    @api_call_wrapper()
    async def terminate(self, reason: str = '') -> None:
        """Terminate the agreement."""
        #   FIXME: check our state first
        await self.api.terminate_agreement(self.id, request_body={"message": reason})
        self.node.event_bus.emit(ResourceClosed(self))
