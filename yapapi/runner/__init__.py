"""

"""
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from types import MappingProxyType
from typing import (
    Optional,
    TypeVar,
    Generic,
    AsyncContextManager,
    Union,
    cast,
    Dict,
    NamedTuple,
    Tuple,
    Mapping,
)
from typing_extensions import Final, Literal
from dataclasses import dataclass, asdict, field
from decimal import Decimal

from .ctx import WorkContext, CommandContainer
from .. import rest
from ..props.builder import DemandBuilder
from ..props import com, Activity, Identification, IdentificationKeys
from ..storage.webdav import DavStorageProvider
import sys
import abc
import aiohttp

if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack


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

    async def map(self, worker, data):
        import asyncio
        import contextlib
        import random

        stack = self._stack
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
        event_queue: asyncio.Queue[Tuple[str, str, Union[None, int, str], dict]] = asyncio.Queue()

        async def _tmp_log():
            while True:
                item = await event_queue.get()
                print(item)

        def emit_progress(
            resource_type: Literal["sub", "prop", "agr", "act", "wkr"],
            event_type: str,
            resource_id: Union[None, int, str] = None,
            **kwargs,
        ):
            event_queue.put_nowait((resource_type, event_type, resource_id, kwargs))

        async def find_offers():
            async with (await builder.subscribe(market_api)) as subscription:
                emit_progress("sub", "created", subscription.id)
                async for proposal in subscription.events():
                    emit_progress("prop", "recv", proposal.id, _from=proposal.issuer)
                    score = await strategy.score_offer(proposal)
                    if score < SCORE_NEUTRAL:
                        proposal_id, provider_id = proposal.id, proposal.issuer
                        with contextlib.suppress(Exception):
                            await proposal.reject()
                        emit_progress("prop", "rejected", proposal_id, _for=provider_id)
                        continue
                    if proposal.is_draft:
                        emit_progress("prop", "buffered", proposal.id)
                        offer_buffer["ala"] = _BufferItem(datetime.now(), score, proposal)
                        offer_buffer[proposal.issuer] = _BufferItem(datetime.now(), score, proposal)
                    else:
                        emit_progress("prop", "respond", proposal.id)
                        await proposal.respond(builder.props, builder.cons)

        workers = []
        last_wid = 0

        aio_session = await self._stack.enter_async_context(aiohttp.ClientSession())

        storage_manager = await DavStorageProvider.for_directory(
            aio_session,
            "http://127.0.0.1:8077/",
            "test1",
            auth=aiohttp.BasicAuth("alice", "secret1234"),
        )

        async def start_worker(agreement: rest.market.Agreement):
            nonlocal last_wid
            wid = last_wid
            last_wid += 1

            details = await agreement.details()
            provider_idn = details.view_prov(Identification)
            emit_progress("wkr", "created", wid, agreement=agreement.id, provider_idn=provider_idn)

            async def task_emiter():
                while True:
                    item = await work_queue.get()
                    emit_progress("wkr", "get-work", wid, task=item)
                    item._start()
                    yield item

            async with (await activity_api.new_activity(agreement.id)) as act:
                emit_progress("act", "create", act.id)

                work_context = WorkContext(f"worker-{wid}", storage_manager)
                async for batch in worker(work_context, task_emiter()):
                    await batch.prepare()
                    print("prepared")
                    cc = CommandContainer()
                    batch.register(cc)
                    remote = await act.send(cc.commands())
                    print("new batch !!!", cc.commands(), remote)
                    async for step in remote:
                        emit_progress("wkr", "step", wid, step=step)

            emit_progress("wkr", "done", wid, agreement=agreement.id)

        def on_worker_stop(task: asyncio.Task):
            import traceback

            # import ya_activity

            if task.exception():
                task.print_stack()
                e = task.exception()
                # if isinstance(ya_activity.exceptions.ApiException, e):
                #    e.
                traceback.print_exception(BaseException, e, None)

        async def worker_starter():
            while True:
                await asyncio.sleep(2)
                if offer_buffer:
                    provider_id, b = random.choice(list(offer_buffer.items()))
                    del offer_buffer[provider_id]
                    try:
                        agreement = await b.proposal.agreement()
                        emit_progress(
                            "agr",
                            "create",
                            agreement.id,
                            provider_idn=(await agreement.details()).view_prov(Identification),
                        )
                        await agreement.confirm()
                        emit_progress("agr", "confirm", agreement.id)
                        task = loop.create_task(start_worker(agreement))
                        workers.append(task)
                        task.add_done_callback(on_worker_stop)
                    except Exception as e:
                        print("fail:", e)
                        raise
                    finally:
                        pass

        async def fill_work_q():
            for task in data:
                await work_queue.put(task)

        loop = asyncio.get_event_loop()
        find_offers_task = loop.create_task(find_offers())
        find_offers_task.add_done_callback(on_worker_stop)
        # Py38: find_offers_task.set_name('find_offers_task')
        try:
            await asyncio.gather(worker_starter(), find_offers_task, _tmp_log(), fill_work_q())
        finally:
            find_offers_task.cancel()
            await find_offers_task
        yield {}

        yield {"done": True}
        pass

    async def __aenter__(self):
        stack = self._stack

        # TODO: Cleanup on exception here.
        self._expires = datetime.now(timezone.utc) + self._conf.timeout
        market_client = await stack.enter_async_context(self._api_config.market())
        self._market_api = rest.Market(market_client)

        activity_client = await stack.enter_async_context(self._api_config.activity())
        print(f"act_url={self._api_config.activity_url}")
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
        if timeout:
            self._expires = self._started + timeout
        else:
            self._expires = expires

        self._result: Optional[TaskResult] = None
        self._data = data
        self._status: TaskStatus = TaskStatus.WAITING

    def _start(self):
        self._status = TaskStatus.RUNNING

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
        assert self._status == TaskStatus.RUNNING
        self._status = TaskStatus.ACCEPTED

    def reject_task(self):
        assert self._status == TaskStatus.RUNNING
        self._status = TaskStatus.REJECTED


class Package(abc.ABC):
    @abc.abstractmethod
    async def resolve_url(self) -> str:
        pass

    @abc.abstractmethod
    async def decorate_demand(self, demand: DemandBuilder):
        pass
