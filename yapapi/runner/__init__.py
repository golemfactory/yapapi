"""

"""
import abc
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, auto
import itertools
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
    Tuple,
    Iterator,
    ClassVar,
)
import traceback
import asyncio

from dataclasses import dataclass, asdict, field
from typing_extensions import Final, AsyncGenerator

from .ctx import WorkContext, CommandContainer, Work
from .events import (
    EventEmitter,
    EventType,
    SubscriptionEvent,
    ProposalEvent,
    AgreementEvent,
    WorkerEvent,
    TaskEvent,
    log_event,
)
from .utils import AsyncWrapper
from .. import rest
from ..props import com, Activity, Identification, IdentificationKeys
from ..props.builder import DemandBuilder
from ..storage import gftp
from ._smartq import SmartQueue, Handle


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
        try:
            linear: com.ComLinear = com.ComLinear.from_props(offer.props)
        except Exception:
            return SCORE_REJECTED

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
        try:
            linear: com.ComLinear = com.ComLinear.from_props(offer.props)
        except Exception:
            return SCORE_REJECTED

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
    """Requestor engine. Used to run tasks based on a common package on providers.
    """

    def __init__(
        self,
        *,
        package: "Package",
        max_workers: int = 5,
        timeout: timedelta = timedelta(minutes=5),
        budget: Union[float, Decimal],
        strategy: MarketStrategy = DummyMS(),
        subnet_tag: Optional[str] = None,
        event_emitter: EventEmitter[EventType] = log_event,
    ):
        """Create a new requestor engine.

        :param package: a package common for all tasks; vm.repo() function may be
                        used to return package from a repository
        :param max_workers: maximum number of workers doing the computation
        :param timeout: timeout for the whole computation
        :param budget: maximum budget for payments
        :param strategy: market strategy used to select providers from the market
                         (e.g. LeastExpensiveLinearPayuMS or DummyMS)
        :param subnet_tag: use only providers in the subnet with the subnet_tag name
        :param event_emitter: an EventEmitter that emits events related to the
                              computation; by default it is a function that logs all events
        """
        self._subnet: Optional[str] = subnet_tag
        self._strategy = strategy
        self._api_config = rest.Configuration()
        self._stack = AsyncExitStack()
        self._package = package
        self._conf = _EngineConf(max_workers, timeout)
        # TODO: setup precitsion
        self._budget_amount = Decimal(budget)
        self._budget_allocation: Optional[rest.payment.Allocation] = None
        # Add buffering to the provided event emitter to make sure
        # that emitting events will not block
        self._wrapped_emitter = AsyncWrapper(event_emitter)

    async def map(
        self,
        worker: Callable[[WorkContext, AsyncIterator["Task"]], AsyncGenerator[Work, None]],
        data,
    ):
        """Run computations on providers.

        :param worker: a callable that takes a WorkContext object and a list o tasks,
                       adds commands to the context object and yields committed commands
        :param data: an iterator of Task objects to be computed on providers
        :return: yields computation progress events
        """
        import asyncio
        import contextlib
        import random

        stack = self._stack
        emit_progress = cast(EventEmitter[EventType], self._wrapped_emitter.async_call)

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
        work_queue = SmartQueue(data)
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
                    emit_progress(
                        AgreementEvent.INVOICE_RECEIVED,
                        invoice.agreement_id,
                        invoice_id=invoice.invoice_id,
                        issuer=invoice.issuer_id,
                        amount=invoice.amount,
                    )
                    agreements_to_pay.remove(invoice.agreement_id)
                    await invoice.accept(amount=invoice.amount, allocation=allocation)
                else:
                    invoices[invoice.agreement_id] = invoice
                if payment_closing and not agreements_to_pay:
                    break

        async def accept_payment_for_agreement(agreement_id: str, *, partial=False) -> bool:
            assert self._budget_allocation
            allocation: rest.payment.Allocation = self._budget_allocation
            emit_progress(AgreementEvent.PAYMENT_PREPARED, agreement_id)
            inv = invoices.get(agreement_id)
            if inv is None:
                agreements_to_pay.add(agreement_id)
                emit_progress(AgreementEvent.PAYMENT_QUEUED, agreement_id)
                return False
            del invoices[agreement_id]
            emit_progress(
                AgreementEvent.PAYMENT_ACCEPTED,
                agreement_id,
                invoice_id=inv.invoice_id,
                issuer=inv.issuer_id,
                amount=inv.amount,
            )
            await inv.accept(amount=inv.amount, allocation=allocation)
            return True

        async def find_offers():
            try:
                subscription = await builder.subscribe(market_api)
            except Exception:
                emit_progress(SubscriptionEvent.FAILED)
                raise
            async with subscription:
                emit_progress(SubscriptionEvent.CREATED, subscription.id)
                try:
                    events = subscription.events()
                except Exception:
                    emit_progress(SubscriptionEvent.COLLECT_FAILED, subscription.id)
                    raise
                async for proposal in events:
                    emit_progress(ProposalEvent.RECEIVED, proposal.id, from_=proposal.issuer)
                    score = await strategy.score_offer(proposal)
                    if score < SCORE_NEUTRAL:
                        proposal_id, provider_id = proposal.id, proposal.issuer
                        with contextlib.suppress(Exception):
                            await proposal.reject()
                        emit_progress(ProposalEvent.REJECTED, proposal_id, for_=provider_id)
                        continue
                    if proposal.is_draft:
                        emit_progress(ProposalEvent.BUFFERED, proposal.id)
                        offer_buffer[proposal.issuer] = _BufferItem(datetime.now(), score, proposal)
                    else:
                        try:
                            await proposal.respond(builder.props, builder.cons)
                        except Exception:
                            emit_progress(ProposalEvent.FAILED, proposal.id)
                            continue
                        emit_progress(ProposalEvent.RESPONDED, proposal.id)

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
            emit_progress(
                WorkerEvent.CREATED,
                wid,
                agreement=agreement.id,
                provider_idn=details.view_prov(Identification),
            )

            try:
                act = await activity_api.new_activity(agreement.id)
            except Exception:
                emit_progress(WorkerEvent.ACTIVITY_CREATE_FAILED, wid, agreement_id=agreement.id)
                raise
            async with act:
                emit_progress(WorkerEvent.ACTIVITY_CREATED, act.id, worker_id=wid)

                work_context = WorkContext(f"worker-{wid}", storage_manager, emitter=emit_progress)
                with work_queue.new_consumer() as consumer:
                    command_generator = worker(
                        work_context,
                        (
                            Task.for_handle(handle, work_queue, emit_progress)
                            async for handle in consumer
                        ),
                    )
                    async for batch in command_generator:
                        try:
                            current_worker_task = consumer.last_item
                            await batch.prepare()
                            cc = CommandContainer()
                            batch.register(cc)
                            remote = await act.send(cc.commands())
                            if current_worker_task:
                                emit_progress(
                                    TaskEvent.SCRIPT_SENT,
                                    current_worker_task.id,
                                    cmds=cc.commands(),
                                    remote=remote,
                                )
                            async for step in remote:
                                emit_progress(
                                    TaskEvent.COMMAND_EXECUTED,
                                    current_worker_task.id,
                                    worker_id=wid,
                                    message=step.message,
                                    idx=step.idx,
                                )
                            emit_progress(
                                TaskEvent.GETTING_RESULTS, current_worker_task.id, worker_id=wid
                            )
                            await batch.post()
                            emit_progress(
                                TaskEvent.SCRIPT_FINISHED, current_worker_task.id, worker_id=wid
                            )
                            await accept_payment_for_agreement(agreement.id, partial=True)
                        except Exception as exc:
                            await command_generator.athrow(Exception, exc)

            await accept_payment_for_agreement(agreement.id)
            emit_progress(WorkerEvent.FINISHED, wid, agreement=agreement.id)

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
                            AgreementEvent.CREATED,
                            agreement.id,
                            provider_idn=(await agreement.details()).view_prov(Identification),
                        )
                        if not await agreement.confirm():
                            emit_progress(AgreementEvent.REJECTED, agreement.id)
                            continue
                        emit_progress(AgreementEvent.CONFIRMED, agreement.id)
                        task = loop.create_task(start_worker(agreement))
                        workers.add(task)
                        # task.add_done_callback(on_worker_stop)
                    except Exception as e:
                        # import traceback
                        # traceback.print_exception(Exception, e, e.__traceback__)
                        if task:
                            task.cancel()
                        emit_progress(ProposalEvent.FAILED, b.proposal.id, reason=str(e))
                    finally:
                        pass

        loop = asyncio.get_event_loop()
        find_offers_task = loop.create_task(find_offers())
        process_invoices_job = loop.create_task(process_invoices())
        wait_until_done = loop.create_task(work_queue.wait_until_done())
        # Py38: find_offers_task.set_name('find_offers_task')
        try:
            services = {
                find_offers_task,
                loop.create_task(worker_starter()),
                process_invoices_job,
                wait_until_done,
            }
            while wait_until_done in services:
                if datetime.now(timezone.utc) > self._expires:
                    raise TimeoutError(f"task timeout exceeded. timeout={self._conf.timeout}")
                done, pending = await asyncio.wait(
                    services.union(workers), timeout=10, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    # if an exception occurred when a service task was running
                    if task in services and not task.cancelled() and task.exception():
                        raise cast(Exception, task.exception())
                    if task in workers:
                        exc = task.exception()
                        log_event(
                            WorkerEvent.FINISHED, None, exception=exc, runnig_workers=len(workers)
                        )
                        try:
                            await task
                        except Exception:
                            traceback.print_exc()

                workers -= done
                services -= done

            yield {"stage": "all work done"}
        finally:
            payment_closing = True
            for worker_task in workers:
                worker_task.cancel()
            find_offers_task.cancel()
            await asyncio.wait(
                workers.union({find_offers_task, process_invoices_job}),
                timeout=5,
                return_when=asyncio.ALL_COMPLETED,
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

        await stack.enter_async_context(self._wrapped_emitter)

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


class Task(Generic[TaskData, TaskResult], object):
    """One computation unit.

    Represents one computation unit that will be run on the provider
    (e.g. rendering of one frame).
    """

    ids: ClassVar[Iterator[int]] = itertools.count(1)

    def __init__(
        self,
        data: TaskData,
        *,
        expires: Optional[datetime] = None,
        timeout: Optional[timedelta] = None,
    ):
        """Create a new Task object.

        :param data: contains information needed to prepare command list for the provider
        :param expires: expiration datetime
        :param timeout: timeout from now; overrides expires parameter if provided
        """
        self.id: int = next(Task.ids)
        self._started = datetime.now()
        self._expires: Optional[datetime]
        self._emit_event: Optional[EventEmitter[TaskEvent]] = None
        self._callbacks: Set[Callable[["Task", str], None]] = set()
        self._handle: Optional[
            Tuple[Handle["Task[TaskData, TaskResult]"], SmartQueue["Task[TaskData, TaskResult]"]]
        ] = None
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
        return f"Task(id={self.id}, data={self._data})"

    def _start(self, emitter: EventEmitter[TaskEvent]):
        self._status = TaskStatus.RUNNING
        self._emit_event = emitter

    def _stop(self, retry: bool = False):
        if self._handle:
            (handle, queue) = self._handle
            if retry:
                asyncio.create_task(queue.reschedule(handle))
            else:
                asyncio.create_task(queue.mark_done(handle))

    @staticmethod
    def for_handle(
        handle: Handle["Task[TaskData, TaskResult]"],
        queue: SmartQueue["Task[TaskData, TaskResult]"],
        emitter: EventEmitter[TaskEvent],
    ) -> "Task[TaskData, TaskResult]":
        task = handle.data
        task._handle = (handle, queue)
        task._start(emitter)
        return task

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
        """Accept task that was completed.

        Must be called when the results of a task are correct and it shouldn't be retried.

        :param result: computation result (optional)
        :return: None
        """
        if self._emit_event:
            self._emit_event(TaskEvent.ACCEPTED, result=result)
        assert self._status == TaskStatus.RUNNING, "Task not running"
        self._status = TaskStatus.ACCEPTED
        self._stop()
        for cb in self._callbacks:
            cb(self, "accept")

    def reject_task(self, reason: Optional[str] = None, retry: bool = False):
        """Reject task.

        Must be called when the results of the task
        are not correct and it should be retried.

        :param reason: task rejection description (optional)
        :return: None
        """
        if self._emit_event:
            self._emit_event(TaskEvent.REJECTED, reason=reason)
        assert self._status == TaskStatus.RUNNING, "Rejected task was not running"
        self._status = TaskStatus.REJECTED
        self._stop(retry)

        for cb in self._callbacks:
            cb(self, "reject")


class Package(abc.ABC):
    @abc.abstractmethod
    async def resolve_url(self) -> str:
        pass

    @abc.abstractmethod
    async def decorate_demand(self, demand: DemandBuilder):
        pass
