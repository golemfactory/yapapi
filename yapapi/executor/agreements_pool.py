import asyncio
from dataclasses import dataclass
import datetime
import random
from typing import Dict, NamedTuple, Optional
from yapapi.executor import events
from yapapi.props import NodeInfo
from yapapi.rest.market import Agreement, OfferProposal


class _BufferedProposal(NamedTuple):
    ts: datetime.datetime
    score: float
    proposal: OfferProposal


@dataclass
class BufferedAgreement:
    agreement: Agreement
    worker_task: Optional[asyncio.Task]
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
        for agreement_id in set(self._agreements):
            try:
                buffered_agreement = self._agreements[agreement_id]
            except IndexError:
                continue
            task = buffered_agreement.worker_task
            if task is not None and task.done():
                await self.release_agreement(buffered_agreement.agreement.id)

    async def add_proposal(self, score: float, proposal: OfferProposal) -> None:
        async with self._lock:
            self._offer_buffer[proposal.issuer] = _BufferedProposal(
                datetime.datetime.now(), score, proposal
            )

    async def use_agreement(self, cbk):
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
        emit = self.emitter

        try:
            buffered_agreement = random.choice(
                [ba for ba in self._agreements.values() if ba.worker_task is None]
            )
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
            emit(events.ProposalFailed(prop_id=b.proposal.id, reason=str(e)))
            raise
        provider = (await agreement.details()).provider_view.extract(NodeInfo)
        emit(events.AgreementCreated(agr_id=agreement.id, provider_id=provider))
        if not await agreement.confirm():
            emit(events.AgreementRejected(agr_id=agreement.id))
            return None
        self._agreements[agreement.id] = BufferedAgreement(
            agreement=agreement,
            worker_task=None,
            has_multi_activity=False,  # TODO: set multi activity flag based on agreement props
        )
        emit(events.AgreementConfirmed(agr_id=agreement.id))
        self.confirmed += 1
        return agreement

    async def release_agreement(self, agreement_id: str) -> None:
        async with self._lock:
            try:
                buffered_agreement = self._agreements[agreement_id]
            except KeyError:
                return
            buffered_agreement.worker_task = None
            # 1. Check wether agreement has multi activity enabled
            if not buffered_agreement.has_multi_activity:
                del self._agreements[agreement_id]
