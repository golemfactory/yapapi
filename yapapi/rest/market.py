import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from types import TracebackType
from typing import AsyncIterator, Optional, TypeVar, Type, Generator, Any, Generic

import aiohttp
from typing_extensions import Awaitable, AsyncContextManager

from ya_market import ApiClient, ApiException, RequestorApi, models  # type: ignore

from .common import is_intermittent_error, SuppressedExceptions
from ..props import Model, NodeInfo


_ModelType = TypeVar("_ModelType", bound=Model)

logger = logging.getLogger(__name__)


class AgreementDetails(object):
    raw_details: models.Agreement

    @dataclass
    class View:
        """A certain fragment of an agreement's properties."""

        properties: dict

        def extract(self, m: Type[_ModelType]) -> _ModelType:
            """Extract properties for the given model from this view's properties."""
            return m.from_properties(self.properties)

    @property
    def provider_view(self) -> View:
        """Get the view of provider's properties in this Agreement."""
        offer: models.Offer = self.raw_details.offer
        return self.View(properties=offer.properties)

    @property
    def requestor_view(self) -> View:
        """Get the view of requestor's properties in this Agreement."""
        demand: models.Demand = self.raw_details.demand
        return self.View(properties=demand.properties)

    @property
    def agreement_id(self):
        return self.raw_details.agreement_id

    @property
    def provider_node_info(self) -> NodeInfo:
        return self.provider_view.extract(NodeInfo)

    def __init__(self, *, _ref: models.Agreement):
        self.raw_details = _ref


class Agreement(object):
    """Mid-level interface to the REST's Agreement model."""

    def __init__(self, api: RequestorApi, subscription: "Subscription", agreement_id: str):
        self._api = api
        self._subscription = subscription
        self._id = agreement_id
        self._details: Optional[AgreementDetails] = None

    @property
    def id(self) -> str:
        return self._id

    async def details(self, force_refresh=False) -> AgreementDetails:
        """Retrieve and cache the details of the Agreement.
        :param force_refresh: if set to True, the API call to get the details will always be made
        """
        if not self._details or force_refresh:
            self._details = AgreementDetails(_ref=await self._api.get_agreement(self._id))
        return self._details

    async def confirm(self) -> bool:
        """Sign and send the agreement to the provider and then wait for it to be approved.

        :return: True if the agreement has been confirmed, False otherwise
        """
        try:
            await self._api.confirm_agreement(self._id)
            await self._api.wait_for_approval(self._id, timeout=15, _request_timeout=16)
            return True
        except (ApiException, asyncio.TimeoutError, aiohttp.ClientOSError):
            logger.debug("waitForApproval(%s) failed", self._id, exc_info=True)
            return False

    async def terminate(self, reason: dict) -> bool:
        """Terminate the agreement.

        :return: True is the agreement has been successfully terminated, False otherwise.
        """
        try:
            await self._api.terminate_agreement(self._id, request_body=reason)
            logger.debug("terminateAgreement(%s) returned successfully", self._id)
            return True
        except (ApiException, asyncio.TimeoutError, aiohttp.ClientOSError):
            logger.debug("terminateAgreement(%s) failed", self._id, exc_info=True)
            return False


class OfferProposal(object):
    """Mid-level interface to handle the negotiation phase between the parties."""

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

    async def reject(self, reason: str = "Rejected"):
        """Reject the Offer."""
        await self._subscription._api.reject_proposal_offer(
            self._subscription.id, self.id, request_body={"message": reason}, _request_timeout=5
        )

    async def respond(self, props: dict, constraints: str) -> str:
        """Create an agreeement Proposal for a received Offer, based on our Demand."""
        proposal = models.DemandOfferBase(properties=props, constraints=constraints)
        new_proposal = await self._subscription._api.counter_proposal_demand(
            self._subscription.id, self.id, proposal, _request_timeout=5
        )
        return new_proposal

    # TODO: This timeout is for negotiation ?
    async def create_agreement(self, timeout=timedelta(hours=1)) -> Agreement:
        """Create an Agreement based on this Proposal."""
        proposal = models.AgreementProposal(
            proposal_id=self.id,
            valid_to=datetime.now(timezone.utc) + timeout,
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
    """Mid-level interface to REST API's Subscription model."""

    def __init__(
        self,
        api: RequestorApi,
        subscription_id: str,
        _details: Optional[models.Demand] = None,
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
        """
        :return: the Demand for which the Subscription has been registered.
        """
        assert self._details is not None, "expected details on list object"
        return self._details

    async def delete(self):
        """Unsubscribe this Demand from the market."""
        self._open = False
        if not self._deleted:
            await self._api.unsubscribe_demand(self._id)

    async def events(self) -> AsyncIterator[OfferProposal]:
        """Yield counter-proposals based on the incoming, matching Offers."""
        while self._open:

            proposals = []
            try:
                async with SuppressedExceptions(is_intermittent_error):
                    proposals = await self._api.collect_offers(self._id, timeout=5, max_events=10)
            except ApiException as ex:
                if ex.status == 404:
                    logger.debug(
                        "Offer unsubscribed or its subscription expired, subscription_id: %s",
                        self._id,
                    )
                    self._open = False
                    # Prevent calling `unsubscribe` which would result in API error
                    # for expired demand subscriptione
                    self._deleted = True
                    continue
                else:
                    raise

            for proposal in proposals:
                if isinstance(proposal, models.ProposalEvent):
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
    """Mid-level interface to the Market REST API."""

    def __init__(self, api_client: ApiClient):
        self._api: RequestorApi = RequestorApi(api_client)

    def subscribe(self, props: dict, constraints: str) -> AsyncResource[Subscription]:
        """
        Create a subscription for a demand specified by the supplied properties and constraints.
        """
        request = models.DemandOfferBase(properties=props, constraints=constraints)

        async def create() -> Subscription:
            sub_id = await self._api.subscribe_demand(request)
            return Subscription(self._api, sub_id)

        return AsyncResource(create())

    async def subscriptions(self) -> AsyncIterator[Subscription]:
        """Yield all the subscriptions that this requestor agent has on the market."""
        for s in (
            Subscription(self._api, demand.demand_id, _details=demand)
            for demand in await self._api.get_demands()
        ):
            yield s


__all__ = ("Market", "Subscription", "OfferProposal", "Agreement")
