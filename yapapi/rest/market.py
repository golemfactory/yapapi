import asyncio
from types import TracebackType
from typing import AsyncIterator, Optional, TypeVar, Type, Generator, Any, Generic

from typing_extensions import Awaitable, AsyncContextManager

from ..props import Model
from ya_market import ApiClient, RequestorApi, models  # type: ignore
from datetime import datetime, timedelta, timezone

_ModelType = TypeVar("_ModelType", bound=Model)


class AgreementDetails(object):
    raw_details: models.Agreement

    def __init__(self, *, _ref: models.Agreement):
        self.raw_details = _ref

    def view_prov(self, c: Type[_ModelType]) -> _ModelType:
        offer: models.Offer = self.raw_details.offer
        return c.from_props(offer.properties)


class Agreement(object):
    def __init__(self, api: RequestorApi, subscription: "Subscription", agreement_id: str):
        self._api = api
        self._subscription = subscription
        self._id = agreement_id

    @property
    def id(self) -> str:
        return self._id

    async def details(self) -> AgreementDetails:
        return AgreementDetails(_ref=await self._api.get_agreement(self._id))

    async def confirm(self) -> bool:
        await self._api.confirm_agreement(self._id)
        msg = await self._api.wait_for_approval(self._id, timeout=90, _request_timeout=100)
        return isinstance(msg, str) and msg.strip().lower() == "approved"


class OfferProposal(object):

    __slots__ = ("_proposal", "_subscription")

    def __init__(self, subscription: "Subscription", proposal: models.ProposalEvent):
        self._proposal: models.ProposalEvent = proposal
        self._subscription: "Subscription" = subscription

    @property
    def issuer(self) -> str:
        return self._proposal.proposal.issuer_id or ""

    @property
    def id(self) -> str:
        return self._proposal.proposal.proposal_id or ""

    @property
    def props(self):
        return self._proposal.proposal.properties

    @property
    def is_draft(self) -> bool:
        return self._proposal.proposal.state == "Draft"

    async def reject(self, reason: Optional[str] = None):
        await self._subscription._api.reject_proposal_offer(self._subscription.id, self.id)

    async def respond(self, props: dict, constraints: str) -> str:
        proposal = models.Proposal(properties=props, constraints=constraints)
        new_proposal = await self._subscription._api.counter_proposal_demand(
            self._subscription.id, self.id, proposal
        )
        return new_proposal

    # TODO: This timeout is for negotiation ?
    async def agreement(self, timeout=timedelta(hours=1)) -> Agreement:
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
    def __init__(
        self, api: RequestorApi, subscription_id: str, _details: Optional[models.Demand] = None,
    ):
        self._api: RequestorApi = api
        self._id: str = subscription_id
        self._open: bool = True
        self._deleted = False
        self._details = _details

    @property
    def id(self):
        return self._id

    def close(self):
        self._open = False

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.delete()

    @property
    def details(self) -> models.Demand:
        assert self._details is not None, "expected details on list object"
        return self._details

    async def delete(self):
        self._open = False
        if not self._deleted:
            await self._api.unsubscribe_demand(self._id)

    async def events(self) -> AsyncIterator[OfferProposal]:
        while self._open:
            proposals = await self._api.collect_offers(self._id, timeout=10, max_events=10)
            for proposal in proposals:
                yield OfferProposal(self, proposal)

            if not proposals:
                await asyncio.sleep(1)


ResourceType = TypeVar("ResourceType", bound=AsyncContextManager[Any])


class AsyncResource(Generic[ResourceType]):
    def __init__(self, _fut: Awaitable[ResourceType]):
        self.__fut: Awaitable[ResourceType] = _fut
        self.__obj: Optional[ResourceType] = None

    def __await__(self) -> Generator[Any, None, ResourceType]:
        return self.__fut.__await__()

    async def __aenter__(self) -> ResourceType:
        self.__obj = await self.__fut
        return self.__obj

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if self.__obj:
            return await self.__obj.__aexit__(exc_type, exc_value, traceback)
        return None


class Market(object):
    def __init__(self, api_client: ApiClient):
        self._api: RequestorApi = RequestorApi(api_client)

    def subscribe(self, props: dict, constraints: str) -> AsyncResource[Subscription]:

        request = models.Demand(properties=props, constraints=constraints)

        async def create() -> Subscription:
            sub_id = await self._api.subscribe_demand(request)
            return Subscription(self._api, sub_id)

        return AsyncResource(create())

    async def subscriptions(self) -> AsyncIterator[Subscription]:
        for s in (
            Subscription(self._api, demand.demand_id, _details=demand)
            for demand in await self._api.get_demands()
        ):
            yield s


__all__ = ("Market", "Subscription", "OfferProposal", "Agreement")
