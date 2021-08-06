import asyncio
from dataclasses import dataclass
import datetime
import logging
import random
import sys
from typing import Dict, NamedTuple, Optional, Set, Tuple, Callable

import aiohttp

from yapapi import events
from yapapi.props import Activity, NodeInfo
from yapapi.rest.market import Agreement, AgreementDetails, ApiException, OfferProposal

logger = logging.getLogger(__name__)


class _BufferedProposal(NamedTuple):
    """Providers' proposal with additional local metadata"""

    ts: datetime.datetime
    score: float
    proposal: OfferProposal


@dataclass
class BufferedAgreement:
    """Confirmed agreement with additional local metadata"""

    agreement: Agreement
    agreement_details: AgreementDetails
    worker_task: Optional[
        asyncio.Task
    ]  # A Task that uses agreement. Agreement won't be reused until this task is .done()
    has_multi_activity: bool


class AgreementsPool:
    """Manages proposals and agreements pool"""

    def __init__(self, job_id: str, emitter: Callable[[events.Event], None]):
        self.job_id = job_id
        self.emitter = emitter
        self._offer_buffer: Dict[str, _BufferedProposal] = {}  # provider_id -> Proposal
        self._agreements: Dict[str, BufferedAgreement] = {}  # agreement_id -> Agreement
        self._lock = asyncio.Lock()
        # The set of provider ids for which the last agreement creation failed
        self._rejecting_providers: Set[str] = set()
        self.confirmed = 0

    async def cycle(self):
        """Performs cyclic tasks.

        Should be called regularly.
        """
        for agreement_id in frozenset(self._agreements):
            try:
                buffered_agreement = self._agreements[agreement_id]
            except IndexError:
                continue
            task = buffered_agreement.worker_task
            if task is not None and task.done():
                await self.release_agreement(
                    buffered_agreement.agreement.id, allow_reuse=task.exception() is None
                )

    async def add_proposal(self, score: float, proposal: OfferProposal) -> None:
        """Adds providers' proposal to the pool of available proposals"""
        async with self._lock:
            self._offer_buffer[proposal.issuer] = _BufferedProposal(
                datetime.datetime.now(), score, proposal
            )

    async def use_agreement(
        self, cbk: Callable[[Agreement, AgreementDetails], asyncio.Task]
    ) -> Optional[asyncio.Task]:
        """Get an agreement and start the `cbk()` task within it."""
        async with self._lock:
            agreement_with_info = await self._get_agreement()
            if agreement_with_info is None:
                return None
            agreement, agreement_details = agreement_with_info
            task = cbk(agreement, agreement_details)
            await self._set_worker(agreement.id, task)
            return task

    async def _set_worker(self, agreement_id: str, task: asyncio.Task) -> None:
        try:
            buffered_agreement = self._agreements[agreement_id]
        except IndexError:
            return
        assert buffered_agreement.worker_task is None
        buffered_agreement.worker_task = task

    async def _get_agreement(self) -> Optional[Tuple[Agreement, AgreementDetails]]:
        """Returns an Agreement

        Firstly it tries to reuse agreement from a pool of available agreements
        (no active worker_task). If that fails it tries to convert offer into agreement.
        """
        emit = self.emitter

        try:
            buffered_agreement = random.choice(
                [ba for ba in self._agreements.values() if ba.worker_task is None]
            )
            logger.debug("Reusing agreement. id: %s", buffered_agreement.agreement.id)
            return buffered_agreement.agreement, buffered_agreement.agreement_details
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
            emit(
                events.ProposalFailed(
                    job_id=self.job_id, prop_id=offer.proposal.id, exc_info=exc_info
                )
            )
            raise
        try:
            agreement_details = await agreement.details()
            provider_activity = agreement_details.provider_view.extract(Activity)
            requestor_activity = agreement_details.requestor_view.extract(Activity)
            node_info = agreement_details.provider_view.extract(NodeInfo)
            logger.debug("New agreement. id: %s, provider: %s", agreement.id, node_info)
            emit(
                events.AgreementCreated(
                    job_id=self.job_id,
                    agr_id=agreement.id,
                    provider_id=provider_id,
                    provider_info=node_info,
                )
            )
        except (ApiException, asyncio.TimeoutError, aiohttp.ClientOSError):
            logger.debug("Cannot get agreement details. id: %s", agreement.id, exc_info=True)
            emit(events.AgreementRejected(job_id=self.job_id, agr_id=agreement.id))
            return None
        if not await agreement.confirm():
            emit(events.AgreementRejected(job_id=self.job_id, agr_id=agreement.id))
            self._rejecting_providers.add(provider_id)
            return None
        self._rejecting_providers.discard(provider_id)
        self._agreements[agreement.id] = BufferedAgreement(
            agreement=agreement,
            agreement_details=agreement_details,
            worker_task=None,
            has_multi_activity=bool(
                provider_activity.multi_activity and requestor_activity.multi_activity
            ),
        )
        emit(events.AgreementConfirmed(job_id=self.job_id, agr_id=agreement.id))
        self.confirmed += 1
        return agreement, agreement_details

    async def release_agreement(self, agreement_id: str, allow_reuse: bool = True) -> None:
        """Marks agreement as unused.

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

        try:
            agreement_details = await buffered_agreement.agreement.details()
            provider = agreement_details.provider_view.extract(NodeInfo).name
        except (ApiException, asyncio.TimeoutError, aiohttp.ClientOSError):
            logger.debug("Cannot get details for agreement %s", agreement_id, exc_info=True)
            provider = "<couldn't get provider name>"

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

        del self._agreements[agreement_id]
        self.emitter(
            events.AgreementTerminated(job_id=self.job_id, agr_id=agreement_id, reason=reason)
        )

    async def terminate_all(self, reason: dict) -> None:
        """Terminate all agreements."""

        async with self._lock:
            for agreement_id in frozenset(self._agreements):
                await self._terminate_agreement(agreement_id, reason)

    async def on_agreement_terminated(self, agr_id: str, reason: dict) -> None:
        """Reacts to agreement termination event

        Should be called when AgreementTerminated event is received.
        """

        async with self._lock:
            try:
                buffered_agreement = self._agreements[agr_id]
            except KeyError:
                return
            buffered_agreement.worker_task and buffered_agreement.worker_task.cancel()
            del self._agreements[agr_id]
            self.emitter(
                events.AgreementTerminated(job_id=self.job_id, agr_id=agr_id, reason=reason)
            )

    def rejected_last_agreement(self, provider_id: str) -> bool:
        """Return `True` iff the last agreement proposed to `provider_id` has been rejected."""
        return provider_id in self._rejecting_providers
