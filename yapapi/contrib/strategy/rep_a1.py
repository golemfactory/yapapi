from typing import Dict, Optional, Set, TYPE_CHECKING
from decimal import Decimal
from collections import defaultdict
import logging

import aiohttp

from yapapi.strategy import WrappingMarketStrategy
from yapapi import events

if TYPE_CHECKING:
    from yapapi.rest.payment import DebitNote, Invoice
    from yapapi.rest.market import OfferProposal


logger = logging.getLogger(__name__)

ActivityId = str
AgreementId = str


PROVIDER_STANDARD_SCORE_URL = "http://reputation.dev.golem.network/standard_score/provider/{}"


def log(msg, *args, **kwargs):
    msg = "\033[94m" + msg + "\033[0m"
    logger.info(msg, *args, **kwargs)


async def get_provider_standard_score(provider_id: str) -> Optional[float]:
    url = PROVIDER_STANDARD_SCORE_URL.format(provider_id)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                score_str = (await response.json())['score']
                score = float(score_str) if score_str is not None else None
                return score
    except aiohttp.client_exceptions.ClientError:
        logger.exception("Reputation service is down")
        return None


class RepA1(WrappingMarketStrategy):
    def __init__(self, base_strategy):
        super().__init__(base_strategy)

        self._failed_activities: Set[ActivityId] = set()
        self._accepted_amounts: Dict[ActivityId, Decimal] = defaultdict(Decimal)
        self._agreement_activity_map: Dict[AgreementId, Set[ActivityId]] = defaultdict(set)

    #################
    #   OFFER SCORING
    async def score_offer(self, offer: "OfferProposal") -> float:
        offer_score = await super().score_offer(offer)
        provider_score = await get_provider_standard_score(offer.issuer)
        combined_score = self._final_score(offer_score, provider_score)

        provider_name = offer._proposal.proposal.properties['golem.node.id.name']
        log(
            "Scored %s -  base: %s, provider: %s, combined: %s",
            provider_name, offer_score, provider_score, combined_score
        )
        return combined_score

    def _final_score(self, base_score: float, provider_score: Optional[float]) -> float:
        #   NOTE: this logic is just a POC
        if provider_score is None:
            provider_score = 0

        if provider_score < -1.5:
            return -1
        return base_score + provider_score

    ######################
    #   PAYMENT MANAGEMENT
    def on_event(self, event: events.Event):
        if isinstance(event, events.ActivityEvent):
            self._agreement_activity_map[event.agreement.id].add(event.activity.id)

        if isinstance(event, events.WorkerFinished):
            if event.exception is not None:
                self._activity_failed(event, "WORKER EXCEPTION")
        elif isinstance(event, events.TaskRejected):
            self._activity_failed(event, "TASK REJECTED")

        elif isinstance(event, events.DebitNoteAccepted):
            activity_id = event.debit_note.activity_id
            prev_accepted_amount = self._accepted_amounts[activity_id]
            new_accepted_amount = max(prev_accepted_amount, Decimal(event.debit_note.total_amount_due))
            self._accepted_amounts[activity_id] = new_accepted_amount
            log("Accepted debit note, total accepted amount: %s", new_accepted_amount)

    def _activity_failed(self, event: events.ActivityEvent, reason: str):
        activity_id = event.activity.id

        self._failed_activities.add(activity_id)

        provider_name = event.provider_info.name
        provider_id = event.agreement.details.raw_details.offer.provider_id
        msg = "Activity on %s (%s) failed, refusing further debit notes/invoices"
        log(msg, provider_name, provider_id)

    async def debit_note_accepted_amount(self, debit_note: "DebitNote") -> Decimal:
        activity_id = debit_note.activity_id

        if activity_id in self._failed_activities:
            #   NOTE: currently it doesn't really matter what we return here,
            #         as long as it is not debit_note.total_amount_due
            return self._accepted_amounts[activity_id]
        return Decimal(debit_note.total_amount_due)

    async def invoice_accepted_amount(self, invoice: "Invoice") -> Decimal:
        agreement_id = invoice.agreement_id
        if self._agreement_has_failed_activity(agreement_id):
            accepted_amount = self._total_agreement_amount(agreement_id)

            #   NOTE: this will (currently) always be true for a failed activity, but there is no rule
            #   saying provider must send invoice for more than the accepted amount
            if accepted_amount < Decimal(invoice.amount):
                log("REJECTED INVOICE FOR %s, we accept only %s", invoice.amount, accepted_amount)
                return accepted_amount

        log("ACCEPTED INVOICE FOR %s", invoice.amount)
        return Decimal(invoice.amount)

    def _agreement_has_failed_activity(self, agreement_id: str) -> bool:
        return any(act in self._failed_activities for act in self._agreement_activity_map[agreement_id])

    def _total_agreement_amount(self, agreement_id: str) -> Decimal:
        return Decimal(sum(self._accepted_amounts[act] for act in self._agreement_activity_map[agreement_id]))
