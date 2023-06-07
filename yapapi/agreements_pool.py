import asyncio
import datetime
import logging
import random
import sys
from typing import Callable, Dict, NamedTuple, Optional

import aiohttp
from dataclasses import dataclass

from yapapi import events
from yapapi.props import Activity, NodeInfo
from yapapi.rest.market import Agreement, AgreementDetails, ApiException, OfferProposal, Market

logger = logging.getLogger(__name__)


class _BufferedProposal(NamedTuple):
    """Providers' proposal with additional local metadata."""

    ts: datetime.datetime
    score: float
    proposal: OfferProposal


@dataclass
class BufferedAgreement:
    """Confirmed agreement with additional local metadata."""

    agreement: Agreement
    agreement_details: AgreementDetails
    worker_task: Optional[
        asyncio.Task
    ]  # A Task that uses agreement. Agreement won't be reused until this task is .done()
    has_multi_activity: bool


class AgreementsPool:
    """Manages proposals and agreements pool."""

    def __init__(
        self,
        emitter: Callable[..., events.Event],
        offer_recycler: Callable[[OfferProposal], None],
        market_api: Market,
    ):
        self.emitter = emitter
        self.offer_recycler = offer_recycler
        self._offer_buffer: Dict[str, _BufferedProposal] = {}  # provider_id -> Proposal
        self._agreements: Dict[str, BufferedAgreement] = {}  # agreement_id -> Agreement
        self._lock = asyncio.Lock()
        self.confirmed = 0
        self._market_api = market_api

    async def cycle(self):
        """Perform cyclic tasks.

        Should be called regularly.
        """
        for agreement_id in frozenset(self._agreements):
            try:
                buffered_agreement = self._agreements[agreement_id]
            except KeyError:
                continue
            task = buffered_agreement.worker_task
            if task is not None and task.done():
                await self.release_agreement(
                    buffered_agreement.agreement.id, allow_reuse=task.exception() is None
                )

    async def add_proposal(self, score: float, proposal: OfferProposal) -> None:
        """Add providers' proposal to the pool of available proposals."""
        async with self._lock:
            self._offer_buffer[proposal.issuer] = _BufferedProposal(
                datetime.datetime.now(), score, proposal
            )

    async def use_agreement(
        self, cbk: Callable[[Agreement], asyncio.Task], agreement_id: Optional[str] = None
    ) -> Optional[asyncio.Task]:
        """Get an agreement and start the `cbk()` task within it."""
        async with self._lock:
            if not agreement_id:
                agreement = await self._get_agreement()
            else:
                agreement = await self._fetch_existing_agreement(agreement_id)

            print("------- Agreement pool: use_agreement: ", agreement)

            if agreement is None:
                return None
            task = cbk(agreement)
            self._set_worker(agreement.id, task)

            print("------- Agreement pool: worker task set: ", task)

            return task

    def _set_worker(self, agreement_id: str, task: asyncio.Task) -> None:
        try:
            buffered_agreement = self._agreements[agreement_id]
        except KeyError:
            return
        assert buffered_agreement.worker_task is None
        buffered_agreement.worker_task = task

    async def _prepare_agreement(
        self,
        agreement: Agreement,
        proposal: Optional[OfferProposal] = None,
        requires_confirmation: bool = True,
    ) -> Optional[Agreement]:
        try:
            agreement_details = await agreement.get_details()
            provider_activity = agreement_details.provider_view.extract(Activity)
            requestor_activity = agreement_details.requestor_view.extract(Activity)
            node_info = agreement_details.provider_view.extract(NodeInfo)
            logger.debug("New agreement. id: %s, provider: %s", agreement.id, node_info)
            self.emitter(events.AgreementCreated, agreement=agreement)
        except (ApiException, asyncio.TimeoutError, aiohttp.ClientOSError):
            logger.debug("Cannot get agreement details. id: %s", agreement.id, exc_info=True)
            self.emitter(events.AgreementRejected, agreement=agreement)
            if proposal:
                self.offer_recycler(proposal)
            return None
        if requires_confirmation and not await agreement.confirm():
            self.emitter(events.AgreementRejected, agreement=agreement)
            if proposal:
                self.offer_recycler(proposal)
            return None
        self._agreements[agreement.id] = BufferedAgreement(
            agreement=agreement,
            agreement_details=agreement_details,
            worker_task=None,
            has_multi_activity=bool(
                provider_activity.multi_activity and requestor_activity.multi_activity
            ),
        )
        self.emitter(events.AgreementConfirmed, agreement=agreement)
        self.confirmed += 1
        return agreement

    async def _fetch_existing_agreement(self, agreement_id) -> Optional[Agreement]:
        agreement = Agreement(self._market_api._api, agreement_id)
        return await self._prepare_agreement(agreement, requires_confirmation=False)

    async def _get_agreement(self) -> Optional[Agreement]:
        """Return an Agreement.

        Firstly it tries to reuse agreement from a pool of available agreements
        (no active worker_task). If that fails it tries to convert offer into agreement.
        """
        emit = self.emitter

        try:
            buffered_agreement = random.choice(
                [ba for ba in self._agreements.values() if ba.worker_task is None]
            )
            logger.debug("Reusing agreement. id: %s", buffered_agreement.agreement.id)
            return buffered_agreement.agreement
        except IndexError:  # empty pool
            pass

        try:
            offers = list(self._offer_buffer.items())
            # Shuffle the offers before picking one with the max score,
            # in case there's more than one with this score.
            random.shuffle(offers)
            provider_id, offer = max(offers, key=lambda elem: elem[1].score)
        except ValueError:  # empty pool
            return None
        del self._offer_buffer[provider_id]
        try:
            agreement = await offer.proposal.create_agreement()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            exc_info = (type(e), e, sys.exc_info()[2])
            emit(events.ProposalFailed, proposal=offer.proposal, exc_info=exc_info)
            raise

        return await self._prepare_agreement(agreement, offer.proposal)

    async def release_agreement(self, agreement_id: str, allow_reuse: bool = True) -> None:
        """Mark agreement as unused.

        If the agreement supports multiple activities and `allow_reuse` is set then
        it will be returned to the pool and will be available for reuse.
        Otherwise it will be removed from the pool.
        """
        async with self._lock:
            try:
                buffered_agreement = self._agreements[agreement_id]
            except KeyError:
                return
            buffered_agreement.worker_task = None
            # Check whether agreement can be reused
            if not allow_reuse or not buffered_agreement.has_multi_activity:
                reason = {"message": "Work cancelled", "golem.requestor.code": "Cancelled"}
                await self._terminate_agreement(agreement_id, reason)

    async def _terminate_agreement(self, agreement_id: str, reason: dict) -> None:
        """Terminate the agreement with given `agreement_id`."""

        if agreement_id not in self._agreements:
            logger.warning("Trying to terminate agreement not in the pool. id: %s", agreement_id)
            return

        buffered_agreement = self._agreements[agreement_id]
        provider = buffered_agreement.agreement.details.provider_view.extract(NodeInfo).name

        logger.debug(
            "Terminating agreement. id: %s, reason: %s, provider: %s",
            agreement_id,
            reason,
            provider,
        )

        if buffered_agreement.worker_task is not None and not buffered_agreement.worker_task.done():
            logger.debug(
                "Terminating agreement that still has worker. agreement_id: %s, worker: %s",
                buffered_agreement.agreement.id,
                buffered_agreement.worker_task,
            )
            buffered_agreement.worker_task.cancel()

        if buffered_agreement.has_multi_activity:
            if not await buffered_agreement.agreement.terminate(reason):
                logger.debug(
                    "Couldn't terminate agreement. id: %s, provider: %s",
                    buffered_agreement.agreement.id,
                    provider,
                )

        try:
            del self._agreements[agreement_id]
            self.emitter(
                events.AgreementTerminated, agreement=buffered_agreement.agreement, reason=reason
            )
        except KeyError:
            logger.debug(
                "Terminated agreement no longer in the pool. id: %s, provider: %s",
                agreement_id,
                provider,
            )

    async def terminate_all(self, reason: dict) -> None:
        """Terminate all agreements."""

        async with self._lock:
            for agreement_id in frozenset(self._agreements):
                await self._terminate_agreement(agreement_id, reason)

    async def on_agreement_terminated(self, agr_id: str, reason: dict) -> None:
        """React to agreement termination event.

        Should be called when AgreementTerminated event is received.
        """

        async with self._lock:
            try:
                buffered_agreement = self._agreements[agr_id]
            except KeyError:
                return
            buffered_agreement.worker_task and buffered_agreement.worker_task.cancel()
            buffered_agreement.agreement._terminated = True
            del self._agreements[agr_id]
            self.emitter(
                events.AgreementTerminated, agreement=buffered_agreement.agreement, reason=reason
            )
