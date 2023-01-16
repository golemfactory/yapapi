import sys
from asyncio import CancelledError
from decimal import Decimal
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, Optional, Set

from dataclasses import dataclass

from yapapi import events

if TYPE_CHECKING:
    from yapapi.engine import Job
    from yapapi.rest.market import Agreement
    from yapapi.rest.payment import Allocation, Invoice


@dataclass
class AgreementData:
    agreement: "Agreement"
    job: "Job"
    invoice: Optional["Invoice"] = None
    payable: bool = False
    paid: bool = False


import logging

logger = logging.getLogger(__name__)


class InvoiceManager:
    def __init__(self):
        self._agreement_data: Dict[str, AgreementData] = {}

    @property
    def payable_unpaid_agreement_ids(self) -> Set[str]:
        return set(
            ad.agreement.id for ad in self._agreement_data.values() if ad.payable and not ad.paid
        )

    @property
    def has_payable_unpaid_agreements(self) -> bool:
        return any(ad.payable and not ad.paid for ad in self._agreement_data.values())

    def add_agreement(self, job: "Job", agreement: "Agreement") -> None:
        """Inform the InvoiceManager about a new agreement (so that we can use the agreement_id in the future)"""
        ad = self._agreement_data.get(agreement.id)
        if ad:
            #   Currently possible if we're having more than one activity for a single agreement
            #   (We could make some effort to ensure this method is called only once, when the agreement is created,
            #   but it will make the code more complex as we'll have to reach here from the AgreeementsPool)
            assert ad.job is job and ad.agreement is agreement
        else:
            self._agreement_data[agreement.id] = AgreementData(agreement, job)

    def agreement_job(self, agreement_id: str) -> "Job":
        #   NOTE: this has nothing to do with InvoiceManaging and is supposed to disappear
        #         once (if) we have a DebitNote manager
        return self._agreement_data[agreement_id].job

    def add_invoice(self, invoice: "Invoice") -> None:
        ad = self._agreement_data.get(invoice.agreement_id)
        if ad:
            ad.job.emit(events.InvoiceReceived, agreement=ad.agreement, invoice=invoice)
            if ad.invoice or ad.paid:
                #   Invoice is ignored (should be rejected, but that's not implemented yet)
                return
            else:
                ad.invoice = invoice
        else:
            #   We don't know this agreement
            #   -> this invoice has nothing to do with the current execution
            #   -> we just ignore it (maybe it deserves payment, but we've no way to check)
            #   ALSO: we're not even emitting InvoiceReceived event, as it requires and Agreement
            #   object, and a proper Agreement doesn't exist --> possible TODO.
            pass

    def set_payable(self, agreement_id: str) -> None:
        self._agreement_data[agreement_id].payable = True

    async def attempt_payment(
        self,
        agreement_id: str,
        get_allocation: Callable[["Invoice"], "Allocation"],
        get_accepted_amount: Callable[["Invoice"], Awaitable[Decimal]],
    ) -> bool:
        ad = self._agreement_data.get(agreement_id)
        if not ad or not ad.invoice or not ad.payable or ad.paid:
            return False

        invoice = ad.invoice
        try:
            allocation = get_allocation(invoice)
            accepted_amount = await get_accepted_amount(invoice)
            if accepted_amount >= Decimal(invoice.amount):
                await invoice.accept(amount=accepted_amount, allocation=allocation)
                ad.job.emit(
                    events.InvoiceAccepted,
                    agreement=ad.agreement,
                    invoice=invoice,
                )
            else:
                #   We should reject the invoice, but it's not implemented in yagna,
                #   so we just ignore it now
                logger.warning(
                    "Ignored invoice %s for %s, we accept only %s",
                    invoice.invoice_id,
                    invoice.amount,
                    accepted_amount,
                )
        except CancelledError:
            raise
        except Exception:
            ad.job.emit(
                events.PaymentFailed,
                agreement=ad.agreement,
                exc_info=sys.exc_info(),  # type: ignore
            )
            return False
        else:
            ad.paid = True
            return True
