import asyncio
from dataclasses import dataclass
from typing import AsyncIterator, Optional, Iterable
from ya_market import ApiClient, RequestorApi, models
from datetime import datetime, timedelta, timezone


class Agreement(object):
    def __init__(
        self, api: RequestorApi, subscription: "Subscription", agreement_id: str
    ):
        self._api = api
        self._subscription = subscription
        self._id = agreement_id

    @property
    def id(self) -> str:
        return self._id

    async def confirm(self):
        await self._api.confirm_agreement(self._id)


class OfferProposal(object):
    def __init__(self, subscription: "Subscription", proposal: models.ProposalEvent):
        self._proposal: models.ProposalEvent = proposal
        self._subscription: "Subscription" = subscription

    @property
    def issuer(self) -> str:
        return self._proposal.proposal.issuer_id

    @property
    def id(self) -> str:
        return self._proposal.proposal.proposal_id

    @property
    def props(self):
        return self._proposal.proposal.properties

    @property
    def is_draft(self) -> bool:
        return self._proposal.proposal.state == "Draft"

    async def respond(
        self, props: Optional[object] = None, constraints: Optional[str] = None
    ) -> str:
        proposal = models.Proposal(properties=props, constraints=constraints)
        new_proposal = await self._subscription._api.counter_proposal_demand(
            self._subscription.id, self.id, proposal
        )
        return new_proposal

    async def agreement(self, timeout=timedelta(minutes=2)) -> Agreement:
        proposal = models.AgreementProposal(
            proposal_id=self.id, valid_to=datetime.now(timezone.utc) + timeout,
        )
        api: RequestorApi = self._subscription._api
        agreement_id = await api.create_agreement(proposal)
        return Agreement(api, self._subscription, agreement_id)

    def __str__(self):
        proposal = self._proposal.proposal
        return f"""OfferProposal(
            id={proposal.proposal_id}
            state={proposal.state}
            issuer={proposal.issuer_id}
        )"""


class Subscription(object):
    def __init__(self, api: RequestorApi, subscription_id: str):
        self._api: RequestorApi = api
        self._id: str = subscription_id
        self._open: bool = True
        self._deleted = False

    @property
    def id(self):
        return self._id

    def close(self):
        self._open = False

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.delete()

    async def delete(self):
        self._open = False
        if not self._deleted:
            await self._api.unsubscribe_demand(self._id)

    async def events(self) -> AsyncIterator[OfferProposal]:
        while self._open:
            proposals = await self._api.collect_offers(
                self._id, timeout=10, max_events=10
            )
            for proposal in proposals:
                yield OfferProposal(self, proposal)

            if not proposals:
                await asyncio.sleep(1)


class Market(object):
    def __init__(self, api_client: ApiClient):
        self._api: RequestorApi = RequestorApi(api_client)

    async def subscribe(self, props, constraints) -> Subscription:
        sub_id = await self._api.subscribe_demand(
            models.Demand(properties=props, constraints=constraints)
        )
        return Subscription(self._api, sub_id)

    async def subscriptions(self) -> Iterable[Subscription]:
        return (
            Subscription(self._api, demand.demand_id)
            for demand in await self._api.get_demands()
        )
