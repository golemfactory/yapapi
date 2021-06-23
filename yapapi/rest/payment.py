import asyncio
import logging

from ya_payment import Account, ApiClient, RequestorApi
import ya_payment.models as yap
from typing import Optional, AsyncIterator, cast, Iterable, Union, List
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from .common import repeat_on_error, is_intermittent_error, SuppressedExceptions
from .resource import ResourceCtx


logger = logging.getLogger(__name__)


class Invoice(yap.Invoice):
    def __init__(self, _api: RequestorApi, _base: yap.Invoice):
        self.__dict__.update(**_base.__dict__)
        self._api: RequestorApi = _api

    @repeat_on_error(max_tries=5)
    async def accept(self, *, amount: Union[Decimal, str], allocation: "Allocation"):
        acceptance = yap.Acceptance(total_amount_accepted=str(amount), allocation_id=allocation.id)
        await self._api.accept_invoice(self.invoice_id, acceptance)


class DebitNote(yap.DebitNote):
    def __init__(self, _api: RequestorApi, _base: yap.DebitNote):
        self.__dict__.update(**_base.__dict__)
        self._api: RequestorApi = _api

    @repeat_on_error(max_tries=5)
    async def accept(self, *, amount: Union[Decimal, str], allocation: "Allocation"):
        acceptance = yap.Acceptance(total_amount_accepted=str(amount), allocation_id=allocation.id)
        await self._api.accept_debit_note(self.debit_note_id, acceptance)


InvoiceStatus = yap.InvoiceStatus
MarketDecoration = yap.MarketDecoration


@dataclass
class _Link:
    _api: RequestorApi


@dataclass(frozen=True)
class AllocationDetails:
    spent_amount: Decimal
    remaining_amount: Decimal


@dataclass
class Allocation(_Link):
    """Payment reservation for task processing."""

    id: str
    """Allocation object id"""

    amount: Decimal
    "Total amount allocated"

    payment_platform: Optional[str]
    "Payment platform, e.g. NGNT"

    payment_address: Optional[str]
    "Payment address, e.g. 0x123..."

    expires: Optional[datetime]
    "Allocation expiration timestamp"

    @repeat_on_error(max_tries=5)
    async def details(self) -> AllocationDetails:
        details: yap.Allocation = await self._api.get_allocation(self.id)
        return AllocationDetails(
            spent_amount=Decimal(details.spent_amount),
            remaining_amount=Decimal(details.remaining_amount),
        )

    @repeat_on_error(max_tries=5)
    async def delete(self):
        await self._api.release_allocation(self.id)


@dataclass
class _AllocationTask(ResourceCtx[Allocation]):
    _api: RequestorApi
    model: yap.Allocation
    _id: Optional[str] = None

    async def __aenter__(self):
        new_allocation: yap.Allocation = await self._api.create_allocation(self.model)
        self._id = new_allocation.allocation_id
        model = self.model
        assert model.total_amount is not None
        assert model.timeout is not None
        assert self._id is not None

        return Allocation(
            _api=self._api,
            id=self._id,
            payment_platform=model.payment_platform,
            payment_address=model.address,
            amount=model.total_amount,
            expires=model.timeout,
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._id:
            await self._api.release_allocation(self._id)


class Payment(object):

    __slots__ = ("_api",)

    def __init__(self, api_client: ApiClient):
        self._api: RequestorApi = RequestorApi(api_client)

    def new_allocation(
        self,
        amount: Decimal,
        payment_platform: str,
        payment_address: str,
        *,
        expires: Optional[datetime] = None,
        make_deposit: bool = False,
    ) -> ResourceCtx[Allocation]:
        """Creates new allocation.

        - `amount`:  Allocation amount.
        - `expires`: expiration timestamp. by default 30 minutes from now.
        - `make_deposit`: (unimplemented).

        """
        allocation_timeout: datetime = expires or datetime.now(timezone.utc) + timedelta(minutes=30)
        return _AllocationTask(
            _api=self._api,
            model=yap.Allocation(
                # TODO: allocation_id should be readonly.
                allocation_id="",
                payment_platform=payment_platform,
                address=payment_address,
                total_amount=str(amount),
                timeout=allocation_timeout,
                make_deposit=make_deposit,
                # TODO: fix this
                spent_amount="",
                remaining_amount="",
            ),
        )

    async def allocations(self) -> AsyncIterator[Allocation]:
        """Lists all active allocations.

        Example:

        Listing all active allocations

            from yapapi import rest

            async def list_allocations(payment_api: rest.Payment):
                async for allocation in payment_api.allocations():
                    print(f'''allocation: {allocation.id}
                        amount={allocation.amount},
                        expires={allocation.expires}''')


        """
        for alloc_obj in cast(Iterable[yap.Allocation], await self._api.get_allocations()):
            yield Allocation(
                _api=self._api,
                id=alloc_obj.allocation_id,
                amount=Decimal(alloc_obj.total_amount),
                payment_platform=alloc_obj.payment_platform,
                payment_address=alloc_obj.address,
                expires=alloc_obj.timeout,
            )

    async def allocation(self, allocation_id: str) -> Allocation:
        allocation_obj: yap.Allocation = await self._api.get_allocation(allocation_id)
        return Allocation(
            _api=self._api,
            id=allocation_obj.allocation_id,
            amount=Decimal(allocation_obj.total_amount),
            payment_platform=allocation_obj.payment_platform,
            payment_address=allocation_obj.address,
            expires=allocation_obj.timeout,
        )

    async def accounts(self) -> AsyncIterator[Account]:
        for account_obj in cast(Iterable[Account], await self._api.get_requestor_accounts()):
            yield account_obj

    async def decorate_demand(self, ids: List[str]) -> yap.MarketDecoration:
        return await self._api.get_demand_decorations(ids)

    @repeat_on_error(max_tries=5)
    async def debit_note(self, debit_note_id: str) -> DebitNote:
        debit_note = await self._api.get_debit_note(debit_note_id)
        return DebitNote(_api=self._api, _base=debit_note)

    async def invoices(self) -> AsyncIterator[Invoice]:

        for invoice_obj in cast(Iterable[yap.Invoice], await self._api.get_invoices()):
            yield Invoice(_api=self._api, _base=invoice_obj)

    @repeat_on_error(max_tries=5)
    async def invoice(self, invoice_id: str) -> Invoice:
        invoice_obj = await self._api.get_invoice(invoice_id)
        return Invoice(_api=self._api, _base=invoice_obj)

    def incoming_invoices(self) -> AsyncIterator[Invoice]:
        ts = datetime.now(timezone.utc)

        async def fetch(init_ts: datetime):
            ts = init_ts
            while True:
                # In the current version of `ya-aioclient` the method `get_invoice_events`
                # incorrectly accepts `timeout` parameter, while the server uses `pollTimeout`
                # events = await api.get_invoice_events(poll_timeout=5, after_timestamp=ts)
                events = []
                async with SuppressedExceptions(is_intermittent_error):
                    events = await self._api.get_invoice_events(after_timestamp=ts)
                for ev in events:
                    logger.debug("Received invoice event: %r, type: %s", ev, ev.__class__)
                    if isinstance(ev, yap.InvoiceReceivedEvent):
                        ts = ev.event_date
                        if not ev.invoice_id:
                            logger.error("Empty invoice id in event: %r", ev)
                            continue
                        invoice = await self.invoice(ev.invoice_id)
                        yield invoice
                if not events:
                    await asyncio.sleep(1)

        return fetch(ts)

    def incoming_debit_notes(self) -> AsyncIterator[DebitNote]:
        ts = datetime.now(timezone.utc)

        async def fetch(init_ts: datetime):
            ts = init_ts
            while True:
                events = []
                async with SuppressedExceptions(is_intermittent_error):
                    events = await self._api.get_debit_note_events(after_timestamp=ts)
                for ev in events:
                    logger.debug("Received debit note event: %r, type: %s", ev, ev.__class__)
                    if isinstance(ev, yap.DebitNoteReceivedEvent):
                        ts = ev.event_date
                        if not ev.debit_note_id:
                            logger.error("Empty debit note id in event: %r", ev)
                            continue
                        debit_note = await self.debit_note(ev.debit_note_id)
                        yield debit_note
                if not events:
                    await asyncio.sleep(1)

        return fetch(ts)
