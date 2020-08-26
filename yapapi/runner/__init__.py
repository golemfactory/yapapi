"""

"""
import abc
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, auto
from types import MappingProxyType
from typing import (
    Optional,
    TypeVar,
    Generic,
    AsyncContextManager,
    Callable,
    Union,
    cast,
    Dict,
    NamedTuple,
    Mapping,
    AsyncIterator,
    Set,
    Tuple
)

from dataclasses import dataclass, asdict, field
from typing_extensions import Final, Literal

from .ctx import WorkContext, CommandContainer, Work
from .events import (
    AsyncEventBuffer,
    EventEmitter,
    EventType,
    log_event,
)
from .. import rest
from ..props import com, Activity, Identification, IdentificationKeys
from ..props.builder import DemandBuilder
from ..storage import gftp

if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack  # type: ignore


CFG_INVOICE_TIMEOUT: Final[timedelta] = timedelta(minutes=5)
"Time to receive invoice from provider after tasks ended."

SCORE_NEUTRAL: Final[float] = 0.0
SCORE_REJECTED: Final[float] = -1.0
SCORE_TRUSTED: Final[float] = 100.0

CFF_DEFAULT_PRICE_FOR_COUNTER: Final[Mapping[com.Counter, Decimal]] = MappingProxyType(
    {com.Counter.TIME: Decimal("0.002"), com.Counter.CPU: Decimal("0.002") * 10}
)


@dataclass
class _EngineConf:
    max_workers: int = 5
    timeout: timedelta = timedelta(minutes=5)


class MarketStrategy(abc.ABC):
    """Abstract market strategy"""

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        pass

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        return SCORE_REJECTED


@dataclass
class DummyMS(MarketStrategy, object):
    max_for_counter: Mapping[com.Counter, Decimal] = CFF_DEFAULT_PRICE_FOR_COUNTER
    max_fixed: Decimal = Decimal("0.05")
    _activity: Optional[Activity] = field(init=False, repr=False, default=None)

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")
        self._activity = Activity.from_props(demand.props)

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        linear: com.ComLinear = com.ComLinear.from_props(offer.props)

        if linear.scheme != com.BillingScheme.PAYU:
            return SCORE_REJECTED

        if linear.fixed_price > self.max_fixed:
            return SCORE_REJECTED
        for counter, price in linear.price_for.items():
            if counter not in self.max_for_counter:
                return SCORE_REJECTED
            if price > self.max_for_counter[counter]:
                return SCORE_REJECTED

        return SCORE_NEUTRAL


@dataclass
class LeastExpensiveLinearPayuMS(MarketStrategy, object):
    def __init__(self, expected_time_secs: int = 60):
        self._expected_time_secs = expected_time_secs

    async def decorate_demand(self, demand: DemandBuilder) -> None:
        demand.ensure(f"({com.PRICE_MODEL}={com.PriceModel.LINEAR.value})")

    async def score_offer(self, offer: rest.market.OfferProposal) -> float:
        linear: com.ComLinear = com.ComLinear.from_props(offer.props)

        if linear.scheme != com.BillingScheme.PAYU:
            return SCORE_REJECTED

        known_time_prices = {com.Counter.TIME, com.Counter.CPU}

        for counter in linear.price_for.keys():
            if counter not in known_time_prices:
                return SCORE_REJECTED

        if linear.fixed_price < 0:
            return SCORE_REJECTED
        expected_price = linear.fixed_price

        for resource in known_time_prices:
            if linear.price_for[resource] < 0:
                return SCORE_REJECTED
            expected_price += linear.price_for[resource] * self._expected_time_secs

        # The higher the expected price value, the lower the score.
        # The score is always lower than SCORE_TRUSTED and is always higher than 0.
        score = SCORE_TRUSTED * 1.0 / (expected_price + 1.01)

        return score


class _BufferItem(NamedTuple):
    ts: datetime
    score: float
    proposal: rest.market.OfferProposal


class Engine(AsyncContextManager):

    def __init__(
        self,
        *,
        package: "Package",
        max_workers: int = 5,
        timeout: timedelta = timedelta(minutes=5),
        budget: Union[float, Decimal],
        strategy: MarketStrategy = DummyMS(),
        subnet_tag: Optional[str] = None,
    ):
        self._subnet: Optional[str] = subnet_tag
        self._strategy = strategy
        self._api_config = rest.Configuration()
        self._stack = AsyncExitStack()
        self._package = package
        self._conf = _EngineConf(max_workers, timeout)
        # TODO: setup precitsion
        self._budget_amount = Decimal(budget)
        self._budget_allocation: Optional[rest.payment.Allocation] = None

    async def map(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator["Task"]],
            AsyncIterator[Tuple["Task", Work]]
        ],
        data,
        event_emitter: EventEmitter[EventType] = log_event
    ):
        import asyncio
        import contextlib
        import random

        ET = EventType

        stack = self._stack
        tasks_processed = {"c": 0, "s": 0}

        # Add buffering to the provided event emitter to make sure
        # that emitting events will not block
        event_buffer = AsyncEventBuffer(event_emitter)
        await stack.enter_async_context(event_buffer)
        emit_progress = event_buffer.emitter

        def on_work_done(task, status):
            if status == "accept":
                tasks_processed["c"] += 1
            else:
                loop = asyncio.get_event_loop()
                loop.create_task(work_queue.put(task))

        # Creating allocation
        if not self._budget_allocation:
            self._budget_allocation = cast(
                rest.payment.Allocation,
                await stack.enter_async_context(
                    self._payment_api.new_allocation(
                        self._budget_amount, expires=self._expires + CFG_INVOICE_TIMEOUT
                    )
                ),
            )

            yield {
                "allocation": self._budget_allocation.id,
                **asdict(await self._budget_allocation.details()),
            }

        # Building offer
        builder = DemandBuilder()
        builder.add(Activity(expiration=self._expires))
        builder.add(Identification(subnet_tag=self._subnet))
        if self._subnet:
            builder.ensure(f"({IdentificationKeys.subnet_tag}={self._subnet})")
        await self._package.decorate_demand(builder)
        await self._strategy.decorate_demand(builder)

        offer_buffer: Dict[str, _BufferItem] = {}
        market_api = self._market_api
        activity_api: rest.Activity = self._activity_api
        strategy = self._strategy
        work_queue: asyncio.Queue[Task] = asyncio.Queue()

        workers: Set[asyncio.Task[None]] = set()
        last_wid = 0

        agreements_to_pay: Set[str] = set()
        invoices: Dict[str, rest.payment.Invoice] = dict()
        payment_closing: bool = False

        async def process_invoices():
            assert self._budget_allocation
            allocation: rest.payment.Allocation = self._budget_allocation
            async for invoice in self._payment_api.incoming_invoices():
                if invoice.agreement_id in agreements_to_pay:
                    agreements_to_pay.remove(invoice.agreement_id)
                    await invoice.accept(amount=invoice.amount, allocation=allocation)
                else:
                    invoices[invoice.agreement_id] = invoice
                if payment_closing and not agreements_to_pay:
                    break

        async def accept_payment_for_agreement(agreement_id: str, *, partial=False) -> bool:
            assert self._budget_allocation
            allocation: rest.payment.Allocation = self._budget_allocation
            emit_progress(ET.PAYMENT_PREPARED, agreement_id)
            inv = invoices.get(agreement_id)
            if inv is None:
                agreements_to_pay.add(agreement_id)
                emit_progress(ET.PAYMENT_QUEUED, agreement_id)
                return False
            del invoices[agreement_id]
            emit_progress(ET.PAYMENT_ACCEPTED, agreement_id, invoice=inv)
            await inv.accept(amount=inv.amount, allocation=allocation)
            return True

        async def find_offers():
            try:
                subscription = await builder.subscribe(market_api)
            except Exception:
                emit_progress(ET.SUBSCRIPTION_FAILED) # "failed to subscribe to market events"
                raise
            async with subscription:
                emit_progress(ET.SUBSCRIPTION_CREATED, subscription.id)
                try:
                    events = subscription.events()
                except Exception:
                    emit_progress(ET.SUBSCRIPTION_COLLECT_FAILED)  # "failed to collect proposal events")
                    raise
                async for proposal in events:
                    emit_progress(ET.PROPOSAL_RECEIVED, proposal.id, from_=proposal.issuer)
                    score = await strategy.score_offer(proposal)
                    if score < SCORE_NEUTRAL:
                        proposal_id, provider_id = proposal.id, proposal.issuer
                        with contextlib.suppress(Exception):
                            await proposal.reject()
                        emit_progress(ET.PROPOSAL_REJECTED, proposal_id, for_=provider_id)
                        continue
                    if proposal.is_draft:
                        emit_progress(ET.PROPOSAL_BUFFERED, proposal.id)
                        offer_buffer[proposal.issuer] = _BufferItem(datetime.now(), score, proposal)
                    else:
                        try:
                            await proposal.respond(builder.props, builder.cons)
                            emit_progress(ET.PROPOSAL_RESPONDED, proposal.id)
                        except Exception:
                            emit_progress(ET.PROPOSAL_FAILED, "failed")
                            raise

        # aio_session = await self._stack.enter_async_context(aiohttp.ClientSession())
        # storage_manager = await DavStorageProvider.for_directory(
        #    aio_session,
        #    "http://127.0.0.1:8077/",
        #    "test1",
        #    auth=aiohttp.BasicAuth("alice", "secret1234"),
        # )
        storage_manager = await self._stack.enter_async_context(gftp.provider())

        async def start_worker(agreement: rest.market.Agreement):
            nonlocal last_wid
            wid = last_wid
            last_wid += 1

            details = await agreement.details()
            provider_idn = details.view_prov(Identification)
            emit_progress(
                ET.WORKER_CREATED, wid, agreement=agreement.id, provider_idn=provider_idn
            )

            async def task_emitter():
                while True:
                    item = await work_queue.get()
                    item._add_callback(on_work_done)
                    emit_progress(ET.WORKER_GET_WORK, wid, task=item)
                    item._start(emitter=emit_progress)
                    yield item

            try:
                act = await activity_api.new_activity(agreement.id)
            except Exception:
                emit_progress(ET.ACTIVITY_CREATE_FAILED) # "could not be created on provider"
                raise
            async with act:
                emit_progress(ET.ACTIVITY_CREATED, act.id)

                work_context = WorkContext(
                    f"worker-{wid}",
                    storage_manager,
                    emitter=emit_progress
                )
                async for (task, batch) in worker(work_context, task_emitter()):
                    try:
                        await batch.prepare()
                        cc = CommandContainer()
                        batch.register(cc)
                        remote = await act.send(cc.commands())
                        emit_progress(ET.BATCH_SENT, cmds=cc.commands(), remote=remote)
                        async for step in remote:
                            message = step.message[:25] if step.message else None
                            idx = step.idx
                            emit_progress(ET.BATCH_STEP, wid, message=message, idx=idx)
                        emit_progress(ET.BATCH_GET_RESULTS, wid)
                        await batch.post()
                        emit_progress(ET.BATCH_DONE, wid)
                        await accept_payment_for_agreement(agreement.id, partial=True)
                    except Exception as exc:
                        task.reject_task(reason=f"failure: {exc}")
                        raise

            await accept_payment_for_agreement(agreement.id)
            emit_progress(ET.WORKER_DONE, wid, agreement=agreement.id)

        async def worker_starter():
            while True:
                await asyncio.sleep(2)
                if offer_buffer and len(workers) < self._conf.max_workers:
                    provider_id, b = random.choice(list(offer_buffer.items()))
                    del offer_buffer[provider_id]
                    task = None
                    try:
                        agreement = await b.proposal.agreement()
                        emit_progress(
                            ET.AGREEMENT_CREATED,
                            agreement.id,
                            provider_idn=(await agreement.details()).view_prov(Identification),
                        )
                        if not await agreement.confirm():
                            emit_progress(ET.AGREEMENT_REJECTED, agreement.id)
                            continue
                        emit_progress(ET.AGREEMENT_CONFIRMED, agreement.id)
                        task = loop.create_task(start_worker(agreement))
                        workers.add(task)
                        # task.add_done_callback(on_worker_stop)
                    except Exception as e:
                        # import traceback
                        # traceback.print_exception(Exception, e, e.__traceback__)
                        if task:
                            task.cancel()
                        emit_progress(ET.PROPOSAL_FAILED, b.proposal.id, reason=str(e))
                    finally:
                        pass

        async def fill_work_q():
            for task in data:
                tasks_processed["s"] += 1
                await work_queue.put(task)

        loop = asyncio.get_event_loop()
        find_offers_task = loop.create_task(find_offers())
        process_invoices_job = loop.create_task(process_invoices())
        # Py38: find_offers_task.set_name('find_offers_task')
        try:
            task_fill_q = loop.create_task(fill_work_q())
            services = {
                find_offers_task,
                task_fill_q,
                loop.create_task(worker_starter()),
                process_invoices_job,
            }
            while (
                task_fill_q in services
                or not work_queue.empty()
                or tasks_processed["s"] > tasks_processed["c"]
            ):
                if datetime.now(timezone.utc) > self._expires:
                    raise TimeoutError(f"task timeout exceeded. timeout={self._conf.timeout}")

                done, pending = await asyncio.wait(
                    services.union(workers), timeout=10, return_when=asyncio.FIRST_COMPLETED
                )
                workers -= done
                services -= done

            yield {"stage": "all work done"}
        except Exception as e:
            print("fail=", e)
        finally:
            payment_closing = True
            for worker_task in workers:
                worker_task.cancel()
            find_offers_task.cancel()
            await asyncio.wait(
                workers.union({find_offers_task, process_invoices_job}), timeout=5, return_when=asyncio.ALL_COMPLETED
            )
        yield {"stage": "wait for invoices", "agreements_to_pay": agreements_to_pay}
        payment_closing = True
        await asyncio.wait({process_invoices_job}, timeout=15, return_when=asyncio.ALL_COMPLETED)

        yield {"done": True}

    async def __aenter__(self):
        stack = self._stack

        # TODO: Cleanup on exception here.
        self._expires = datetime.now(timezone.utc) + self._conf.timeout
        market_client = await stack.enter_async_context(self._api_config.market())
        self._market_api = rest.Market(market_client)

        activity_client = await stack.enter_async_context(self._api_config.activity())
        self._activity_api = rest.Activity(activity_client)

        payment_client = await stack.enter_async_context(self._api_config.payment())
        self._payment_api = rest.Payment(payment_client)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # self._market_api = None
        # self._payment_api = None
        await self._stack.aclose()


class TaskStatus(Enum):
    WAITING = auto()
    RUNNING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


TaskData = TypeVar("TaskData")
TaskResult = TypeVar("TaskResult")
TaskEvents = Literal[EventType.TASK_ACCEPTED, EventType.TASK_REJECTED]


class Task(Generic[TaskData, TaskResult], object):
    def __init__(
        self,
        data: TaskData,
        *,
        expires: Optional[datetime] = None,
        timeout: Optional[timedelta] = None,
    ):
        self._started = datetime.now()
        self._expires: Optional[datetime]
        self._emit_event: Optional[EventEmitter[TaskEvents]] = None
        self._callbacks: Set[Callable[["Task", str], None]] = set()
        if timeout:
            self._expires = self._started + timeout
        else:
            self._expires = expires

        self._result: Optional[TaskResult] = None
        self._data = data
        self._status: TaskStatus = TaskStatus.WAITING

    def _add_callback(self, callback):
        self._callbacks.add(callback)

    def __repr__(self):
        return f"Task(data={self._data}"

    def _start(self, emitter: EventEmitter[TaskEvents]):
        self._status = TaskStatus.RUNNING
        self._emit_event = emitter

    @property
    def data(self) -> TaskData:
        return self._data

    @property
    def output(self) -> Optional[TaskResult]:
        return self._result

    @property
    def expires(self):
        return self._expires

    def accept_task(self, result: Optional[TaskResult] = None):
        if self._emit_event:
            self._emit_event(EventType.TASK_ACCEPTED, result=result)
        assert self._status == TaskStatus.RUNNING
        self._status = TaskStatus.ACCEPTED
        for cb in self._callbacks:
            cb(self, "accept")

    def reject_task(self, reason: Optional[str] = None):
        if self._emit_event:
            self._emit_event(EventType.TASK_REJECTED, reason=reason)
        assert self._status == TaskStatus.RUNNING
        self._status = TaskStatus.REJECTED
        for cb in self._callbacks:
            cb(self, "reject")


class Package(abc.ABC):
    @abc.abstractmethod
    async def resolve_url(self) -> str:
        pass

    @abc.abstractmethod
    async def decorate_demand(self, demand: DemandBuilder):
        pass
