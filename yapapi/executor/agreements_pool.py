import asyncio
from dataclasses import dataclass
import datetime
import logging
import random
import sys
from typing import Dict, NamedTuple, Optional

from yapapi.executor import events
from yapapi.props import Activity, NodeInfo
from yapapi.rest.market import Agreement, OfferProposal

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
    worker_task: Optional[
        asyncio.Task
    ]  # A Task that uses agreement. Agreement won't be reused until this task is .done()
    has_multi_activity: bool


class AgreementsPool:
    """Manages proposals and agreements pool"""

    def __init__(self, emitter):
        self.emitter = emitter
        self._offer_buffer: Dict[str, _BufferedProposal] = {}  # provider_id -> Proposal
        self._agreements: Dict[str, BufferedAgreement] = {}  # agreement_id -> Agreement
        self._lock = asyncio.Lock()
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
                await self.release_agreement(buffered_agreement.agreement.id)

    async def add_proposal(self, score: float, proposal: OfferProposal) -> None:
        """Adds providers' proposal to the pool of available proposals"""
        async with self._lock:
            self._offer_buffer[proposal.issuer] = _BufferedProposal(
                datetime.datetime.now(), score, proposal
            )

    async def use_agreement(self, cbk):
        """Gets an agreement and performs cbk() on it"""
        async with self._lock:
            agreement = await self._get_agreement()
            if agreement is None:
                return None
            task = cbk(agreement)
            await self._set_worker(agreement.id, task)
            return task

    async def _set_worker(self, agreement_id: str, task: asyncio.Task) -> None:
        try:
            buffered_agreement = self._agreements[agreement_id]
        except IndexError:
            return
        assert buffered_agreement.worker_task is None
        buffered_agreement.worker_task = task

    async def _get_agreement(self) -> Optional[Agreement]:
        """Returns an Agreement

        Firstly it tries to reuse agreement from a pool of available agreements (no active worker_task).
        If that fails it tries to convert offer into agreement.
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

        offer_buffer = self._offer_buffer
        try:
            provider_id, b = random.choice(list(offer_buffer.items()))
        except IndexError:  # empty pool
            return None
        del offer_buffer[provider_id]
        try:
            agreement = await b.proposal.create_agreement()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            emit(events.ProposalFailed(prop_id=b.proposal.id, exc_info=sys.exc_info()))
            raise
        agreement_details = await agreement.details()
        provider_activity = agreement_details.provider_view.extract(Activity)
        requestor_activity = agreement_details.requestor_view.extract(Activity)
        provider = agreement_details.provider_view.extract(NodeInfo)
        logger.debug("New agreement. id: %s, provider: %s", agreement.id, provider)
        emit(events.AgreementCreated(agr_id=agreement.id, provider_id=provider))
        if not await agreement.confirm():
            emit(events.AgreementRejected(agr_id=agreement.id))
            return None
        self._agreements[agreement.id] = BufferedAgreement(
            agreement=agreement,
            worker_task=None,
            has_multi_activity=bool(
                provider_activity.multi_activity and requestor_activity.multi_activity
            ),
        )
        emit(events.AgreementConfirmed(agr_id=agreement.id))
        self.confirmed += 1
        return agreement

    async def release_agreement(self, agreement_id: str) -> None:
        """Marks agreement as ready for reuse"""
        async with self._lock:
            try:
                buffered_agreement = self._agreements[agreement_id]
            except KeyError:
                return
            buffered_agreement.worker_task = None
            # Check whether agreement has multi activity enabled
            if not buffered_agreement.has_multi_activity:
                del self._agreements[agreement_id]
                logger.debug("Removed single-activity agreement. %s", agreement_id)

    async def terminate(self, reason: dict) -> None:
        """Terminates all agreements"""
        async with self._lock:
            for agreement_id in frozenset(self._agreements):
                buffered_agreement = self._agreements[agreement_id]
                agreement_details = await buffered_agreement.agreement.details()
                provider = agreement_details.provider_view.extract(NodeInfo)
                logger.debug(
                    "Terminating agreement. id: %s, reason: %s, provider: %s",
                    agreement_id,
                    reason,
                    provider,
                )
                if (
                    buffered_agreement.worker_task is not None
                    and not buffered_agreement.worker_task.done()
                ):
                    logger.debug(
                        "Terminating agreement that still has worker. agreement_id: %s, worker: %s",
                        buffered_agreement.agreement.id,
                        buffered_agreement.worker_task,
                    )
                    buffered_agreement.worker_task.cancel()
                if buffered_agreement.has_multi_activity:
                    if not await buffered_agreement.agreement.terminate(reason):
                        logger.debug(
                            "Couldn't terminate agreement. id=%s, provider=%s",
                            buffered_agreement.agreement.id,
                            provider,
                        )
                del self._agreements[agreement_id]
                self.emitter(events.AgreementTerminated(agr_id=agreement_id, reason=reason))

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
            self.emitter(events.AgreementTerminated(agr_id=agr_id, reason=reason))
