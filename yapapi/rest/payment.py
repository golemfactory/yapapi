from ya_payment import Account, ApiClient, RequestorApi
import ya_payment.models as yap
from typing import Optional, AsyncIterator, cast, Iterable, Union, List
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from .resource import ResourceCtx


class Invoice(yap.Invoice):
    def __init__(self, _api: RequestorApi, _base: yap.Invoice):
        self.__dict__.update(**_base.__dict__)
        self._api: RequestorApi = _api

    async def accept(self, *, amount: Union[Decimal, str], allocation: "Allocation"):
        acceptance = yap.Acceptance(total_amount_accepted=str(amount), allocation_id=allocation.id)
        await self._api.accept_invoice(self.invoice_id, acceptance)


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

    async def details(self) -> AllocationDetails:
        details: yap.Allocation = await self._api.get_allocation(self.id)
        return AllocationDetails(
            spent_amount=Decimal(details.spent_amount),
            remaining_amount=Decimal(details.remaining_amount),
        )

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
        for account_obj in cast(Iterable[Account], await self._api.get_send_accounts()):
            yield account_obj

    async def decorate_demand(self, ids: List[str]) -> yap.MarketDecoration:
        return await self._api.decorate_demand(ids)

    async def invoices(self) -> AsyncIterator[Invoice]:

        for invoice_obj in cast(Iterable[yap.Invoice], await self._api.get_received_invoices()):
            yield Invoice(_api=self._api, _base=invoice_obj)

    async def invoice(self, invoice_id: str) -> Invoice:
        invoice_obj = await self._api.get_received_invoice(invoice_id)
        return Invoice(_api=self._api, _base=invoice_obj)

    def incoming_invoices(self) -> AsyncIterator[Invoice]:
        ts = datetime.now(timezone.utc)
        api = self._api

        async def fetch(init_ts: datetime):
            ts = init_ts
            while True:
                items = cast(
                    Iterable[yap.InvoiceEvent],
                    await api.get_requestor_invoice_events(timeout=5, later_than=ts),
                )
                for ev in items:
                    ts = ev.timestamp
                    if ev.event_type == yap.EventType.RECEIVED:
                        invoice = await self.invoice(ev.invoice_id)
                        yield invoice

        return fetch(ts)
