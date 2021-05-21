"""
An implementation of the new Golem's task executor.
"""
import asyncio
from asyncio import CancelledError
import contextlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import itertools
import logging
import sys
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)
import uuid
import warnings


from dataclasses import dataclass
from yapapi.executor.agreements_pool import AgreementsPool
from typing_extensions import Final, AsyncGenerator

from .ctx import CaptureContext, CommandContainer, ExecOptions, Work, WorkContext
from .events import Event
from . import events
from .task import Task, TaskStatus
from .utils import AsyncWrapper
from ..payload import Payload
from ..props import Activity, com, NodeInfo, NodeInfoKeys
from ..props.builder import DemandBuilder, DemandDecorator
from .. import rest
from ..rest.activity import CommandExecutionError
from ..rest.market import OfferProposal, Subscription
from ..storage import gftp
from ._smartq import Consumer, Handle, SmartQueue
from .strategy import (
    DecreaseScoreForUnconfirmedAgreement,
    LeastExpensiveLinearPayuMS,
    MarketStrategy,
    SCORE_NEUTRAL,
)

if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack  # type: ignore

DEBIT_NOTE_MIN_TIMEOUT: Final[int] = 30  # in seconds
"Shortest debit note acceptance timeout the requestor will accept."

DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP: Final[str] = "golem.com.payment.debit-notes.accept-timeout?"

CFG_INVOICE_TIMEOUT: Final[timedelta] = timedelta(minutes=5)
"Time to receive invoice from provider after tasks ended."

DEFAULT_EXECUTOR_TIMEOUT: Final[timedelta] = timedelta(minutes=15)
"Joint timeout for all tasks submitted to an executor."

DEFAULT_DRIVER = "zksync"
DEFAULT_NETWORK = "rinkeby"

logger = logging.getLogger(__name__)


class NoPaymentAccountError(Exception):
    """The error raised if no payment account for the required driver/network is available."""

    required_driver: str
    """Payment driver required for the account."""

    required_network: str
    """Network required for the account."""

    def __init__(self, required_driver: str, required_network: str):
        self.required_driver: str = required_driver
        self.required_network: str = required_network

    def __str__(self) -> str:
        return (
            f"No payment account available for driver `{self.required_driver}`"
            f" and network `{self.required_network}`"
        )


WorkItem = Union[Work, Tuple[Work, ExecOptions]]
"""The type of items yielded by a generator created by the `worker` function supplied by user."""


def unpack_work_item(item: WorkItem) -> Tuple[Work, ExecOptions]:
    """Extract `Work` object and options from a work item.
    If the item does not specify options, default ones are provided.
    """
    if isinstance(item, tuple):
        return item
    else:
        return item, ExecOptions()


exescript_ids: Iterator[int] = itertools.count(1)


D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Golem(AsyncContextManager):
    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        app_key: Optional[str] = None,
    ):
        """
        Base execution engine containing functions common to all modes of operation

        :param budget: maximum budget for payments
        :param strategy: market strategy used to select providers from the market
            (e.g. LeastExpensiveLinearPayuMS or DummyMS)
        :param subnet_tag: use only providers in the subnet with the subnet_tag name
        :param driver: name of the payment driver to use or `None` to use the default driver;
            only payment platforms with the specified driver will be used
        :param network: name of the network to use or `None` to use the default network;
            only payment platforms with the specified network will be used
        :param event_consumer: a callable that processes events related to the
            computation; by default it is a function that logs all events
        :param stream_output: stream computation output from providers
        :param app_key: optional Yagna application key. If not provided, the default is to
                        get the value from `YAGNA_APPKEY` environment variable
        """
        self._init_api(app_key)

        self._budget_amount = Decimal(budget)
        self._budget_allocations: List[rest.payment.Allocation] = []

        if not strategy:
            strategy = LeastExpensiveLinearPayuMS(
                max_fixed_price=Decimal("1.0"),
                max_price_for={com.Counter.CPU: Decimal("0.2"), com.Counter.TIME: Decimal("0.1")},
            )
            # The factor 0.5 below means that an offer for a provider that failed to confirm
            # the last agreement proposed to them will have it's score multiplied by 0.5.
            strategy = DecreaseScoreForUnconfirmedAgreement(strategy, 0.5)
        self._strategy = strategy

        self._subnet: Optional[str] = subnet_tag
        self._driver = driver.lower() if driver else DEFAULT_DRIVER
        self._network = network.lower() if network else DEFAULT_NETWORK

        if not event_consumer:
            # Use local import to avoid cyclic imports when yapapi.log
            # is imported by client code
            from ..log import log_event_repr

            event_consumer = log_event_repr

        # Add buffering to the provided event emitter to make sure
        # that emitting events will not block
        self._wrapped_consumer = AsyncWrapper(event_consumer)

        self._stream_output = stream_output

        # initialize the payment structures
        self._agreements_to_pay: Set[str] = set()
        self._agreements_accepting_debit_notes: Set[str] = set()
        self._invoices: Dict[str, rest.payment.Invoice] = dict()
        self._payment_closing: bool = False

        # a set of `Job` instances used to track jobs - computations or services - started
        # it can be used to wait until all jobs are finished
        self._jobs: Set[Job] = set()
        self._process_invoices_job: Optional[asyncio.Task] = None

        self._services: Set[asyncio.Task] = set()
        self._stack = AsyncExitStack()

    async def create_demand_builder(
        self, expiration_time: datetime, payload: Payload
    ) -> DemandBuilder:
        """Create a `DemandBuilder` for given `payload` and `expiration_time`."""
        builder = DemandBuilder()
        builder.add(Activity(expiration=expiration_time, multi_activity=True))
        builder.add(NodeInfo(subnet_tag=self._subnet))
        if self._subnet:
            builder.ensure(f"({NodeInfoKeys.subnet_tag}={self._subnet})")
        await builder.decorate(self.payment_decoration, self.strategy, payload)
        return builder

    def _init_api(self, app_key: Optional[str] = None):
        """
        initialize the REST (low-level) API
        :param app_key: (optional) yagna daemon application key
        """
        self._api_config = rest.Configuration(app_key)

    @property
    def driver(self) -> str:
        return self._driver

    @property
    def network(self) -> str:
        return self._network

    @property
    def storage_manager(self):
        return self._storage_manager

    @property
    def strategy(self) -> MarketStrategy:
        return self._strategy

    def emit(self, *args, **kwargs) -> None:
        if self._wrapped_consumer:
            self._wrapped_consumer.async_call(*args, **kwargs)

    async def __aenter__(self) -> "Golem":
        try:
            stack = self._stack

            await stack.enter_async_context(self._wrapped_consumer)

            def report_shutdown(*exc_info):
                if any(item for item in exc_info):
                    self.emit(events.ShutdownFinished(exc_info=exc_info))  # noqa
                else:
                    self.emit(events.ShutdownFinished())

            stack.push(report_shutdown)

            market_client = await stack.enter_async_context(self._api_config.market())
            self._market_api = rest.Market(market_client)

            activity_client = await stack.enter_async_context(self._api_config.activity())
            self._activity_api = rest.Activity(activity_client)

            payment_client = await stack.enter_async_context(self._api_config.payment())
            self._payment_api = rest.Payment(payment_client)

            self.payment_decoration = Golem.PaymentDecoration(await self._create_allocations())

            # TODO: make the method starting the process_invoices() task an async context manager
            # to simplify code in __aexit__()
            loop = asyncio.get_event_loop()
            self._process_invoices_job = loop.create_task(self.process_invoices())
            self._services.add(self._process_invoices_job)
            self._services.add(loop.create_task(self.process_debit_notes()))

            self._storage_manager = await self._stack.enter_async_context(gftp.provider())

            return self
        except:
            await self.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Importing this at the beginning would cause circular dependencies
        from ..log import pluralize

        try:
            logger.debug("Golem is shutting down...")
            # Wait until all computations are finished
            await asyncio.gather(*[job.finished.wait() for job in self._jobs])

            self._payment_closing = True

            for task in self._services:
                if task is not self._process_invoices_job:
                    task.cancel()

            if self._process_invoices_job and not any(
                True for job in self._jobs if job.agreements_pool.confirmed > 0
            ):
                logger.debug("No need to wait for invoices.")
                self._process_invoices_job.cancel()

            try:
                logger.info("Waiting for Golem services to finish...")
                _, pending = await asyncio.wait(
                    self._services, timeout=10, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.debug(
                        "%s still running: %s", pluralize(len(pending), "service"), pending
                    )
            except Exception:
                logger.debug("Got error when waiting for services to finish", exc_info=True)

            if self._agreements_to_pay and self._process_invoices_job:
                logger.info(
                    "%s still unpaid, waiting for invoices...",
                    pluralize(len(self._agreements_to_pay), "agreement"),
                )
                await asyncio.wait(
                    {self._process_invoices_job}, timeout=30, return_when=asyncio.ALL_COMPLETED
                )
                if self._agreements_to_pay:
                    logger.warning("Unpaid agreements: %s", self._agreements_to_pay)

        finally:
            await self._stack.aclose()

    async def _create_allocations(self) -> rest.payment.MarketDecoration:

        if not self._budget_allocations:
            async for account in self._payment_api.accounts():
                driver = account.driver.lower()
                network = account.network.lower()
                if (driver, network) != (self._driver, self._network):
                    logger.debug(
                        f"Not using payment platform `%s`, platform's driver/network "
                        f"`%s`/`%s` is different than requested driver/network `%s`/`%s`",
                        account.platform,
                        driver,
                        network,
                        self._driver,
                        self._network,
                    )
                    continue
                logger.debug("Creating allocation using payment platform `%s`", account.platform)
                allocation = cast(
                    rest.payment.Allocation,
                    await self._stack.enter_async_context(
                        self._payment_api.new_allocation(
                            self._budget_amount,
                            payment_platform=account.platform,
                            payment_address=account.address,
                            #   TODO what do to with this?
                            #   expires=self._expires + CFG_INVOICE_TIMEOUT,
                        )
                    ),
                )
                self._budget_allocations.append(allocation)

            if not self._budget_allocations:
                raise NoPaymentAccountError(self._driver, self._network)

        allocation_ids = [allocation.id for allocation in self._budget_allocations]
        return await self._payment_api.decorate_demand(allocation_ids)

    def _get_allocation(
        self, item: Union[rest.payment.DebitNote, rest.payment.Invoice]
    ) -> rest.payment.Allocation:
        try:
            return next(
                allocation
                for allocation in self._budget_allocations
                if allocation.payment_address == item.payer_addr
                and allocation.payment_platform == item.payment_platform
            )
        except:
            raise ValueError(f"No allocation for {item.payment_platform} {item.payer_addr}.")

    async def process_invoices(self) -> None:
        async for invoice in self._payment_api.incoming_invoices():
            if invoice.agreement_id in self._agreements_to_pay:
                self.emit(
                    events.InvoiceReceived(
                        agr_id=invoice.agreement_id,
                        inv_id=invoice.invoice_id,
                        amount=invoice.amount,
                    )
                )
                try:
                    allocation = self._get_allocation(invoice)
                    await invoice.accept(amount=invoice.amount, allocation=allocation)
                except CancelledError:
                    raise
                except Exception:
                    self.emit(
                        events.PaymentFailed(
                            agr_id=invoice.agreement_id, exc_info=sys.exc_info()  # type: ignore
                        )
                    )
                else:
                    self._agreements_to_pay.remove(invoice.agreement_id)
                    assert invoice.agreement_id in self._agreements_accepting_debit_notes
                    self._agreements_accepting_debit_notes.remove(invoice.agreement_id)
                    self.emit(
                        events.PaymentAccepted(
                            agr_id=invoice.agreement_id,
                            inv_id=invoice.invoice_id,
                            amount=invoice.amount,
                        )
                    )
            else:
                self._invoices[invoice.agreement_id] = invoice
            if self._payment_closing and not self._agreements_to_pay:
                break

    # TODO Consider processing invoices and debit notes together
    async def process_debit_notes(self) -> None:
        async for debit_note in self._payment_api.incoming_debit_notes():
            if debit_note.agreement_id in self._agreements_accepting_debit_notes:
                self.emit(
                    events.DebitNoteReceived(
                        agr_id=debit_note.agreement_id,
                        amount=debit_note.total_amount_due,
                        note_id=debit_note.debit_note_id,
                    )
                )
                try:
                    allocation = self._get_allocation(debit_note)
                    await debit_note.accept(
                        amount=debit_note.total_amount_due, allocation=allocation
                    )
                except CancelledError:
                    raise
                except Exception:
                    self.emit(
                        events.PaymentFailed(
                            agr_id=debit_note.agreement_id, exc_info=sys.exc_info()  # type: ignore
                        )
                    )
            if self._payment_closing and not self._agreements_to_pay:
                break

    async def accept_payment_for_agreement(
        self, agreement_id: str, *, partial: bool = False
    ) -> None:
        self.emit(events.PaymentPrepared(agr_id=agreement_id))
        inv = self._invoices.get(agreement_id)
        if inv is None:
            self._agreements_to_pay.add(agreement_id)
            self.emit(events.PaymentQueued(agr_id=agreement_id))
            return
        del self._invoices[agreement_id]
        allocation = self._get_allocation(inv)
        await inv.accept(amount=inv.amount, allocation=allocation)
        self.emit(
            events.PaymentAccepted(agr_id=agreement_id, inv_id=inv.invoice_id, amount=inv.amount)
        )

    def approve_agreement_payments(self, agreement_id):
        self._agreements_accepting_debit_notes.add(agreement_id)

    def add_job(self, job: "Job"):
        self._jobs.add(job)

    @staticmethod
    def finalize_job(job: "Job"):
        job.finished.set()

    @dataclass
    class PaymentDecoration(DemandDecorator):
        market_decoration: rest.payment.MarketDecoration

        async def decorate_demand(self, demand: DemandBuilder):
            for constraint in self.market_decoration.constraints:
                demand.ensure(constraint)
            demand.properties.update({p.key: p.value for p in self.market_decoration.properties})

    async def create_activity(self, agreement_id: str):
        return await self._activity_api.new_activity(
            agreement_id, stream_events=self._stream_output
        )

    async def process_batches(
        self,
        agreement_id: str,
        activity: rest.activity.Activity,
        command_generator: AsyncGenerator[Work, Awaitable[List[events.CommandEvent]]],
    ) -> None:
        """Send command batches produced by `command_generator` to `activity`."""

        item = await command_generator.__anext__()

        while True:

            batch, exec_options = unpack_work_item(item)
            # TODO: `task_id` should really be `batch_id`, but then we should also rename
            # `task_id` field of several events (e.g. `ScriptSent`)
            script_id = str(next(exescript_ids))

            if batch.timeout:
                if exec_options.batch_timeout:
                    logger.warning(
                        "Overriding batch timeout set with commit(batch_timeout)"
                        "by the value set in exec options"
                    )
                else:
                    exec_options.batch_timeout = batch.timeout

            batch_deadline = (
                datetime.now(timezone.utc) + exec_options.batch_timeout
                if exec_options.batch_timeout
                else None
            )

            await batch.prepare()
            cc = CommandContainer()
            batch.register(cc)
            remote = await activity.send(cc.commands(), deadline=batch_deadline)
            cmds = cc.commands()
            self.emit(events.ScriptSent(agr_id=agreement_id, script_id=script_id, cmds=cmds))

            async def get_batch_results() -> List[events.CommandEvent]:
                results = []
                async for evt_ctx in remote:
                    evt = evt_ctx.event(agr_id=agreement_id, script_id=script_id, cmds=cmds)
                    self.emit(evt)
                    results.append(evt)
                    if isinstance(evt, events.CommandExecuted) and not evt.success:
                        raise CommandExecutionError(evt.command, evt.message, evt.stderr)

                self.emit(events.GettingResults(agr_id=agreement_id, script_id=script_id))
                await batch.post()
                self.emit(events.ScriptFinished(agr_id=agreement_id, script_id=script_id))
                await self.accept_payment_for_agreement(agreement_id, partial=True)
                return results

            loop = asyncio.get_event_loop()

            if exec_options.wait_for_results:
                # Block until the results are available
                try:
                    future_results = loop.create_future()
                    results = await get_batch_results()
                    future_results.set_result(results)
                    item = await command_generator.asend(future_results)
                except StopAsyncIteration:
                    raise
                except Exception:
                    # Raise the exception in `command_generator` (the `worker` coroutine).
                    # If the client code is able to handle it then we'll proceed with
                    # subsequent batches. Otherwise the worker finishes with error.
                    item = await command_generator.athrow(*sys.exc_info())
            else:
                # Schedule the coroutine in a separate asyncio task
                future_results = loop.create_task(get_batch_results())
                item = await command_generator.asend(future_results)

    async def execute_tasks(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[Work, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        payload: Payload,
        max_workers: Optional[int] = None,
        timeout: Optional[timedelta] = None,
        budget: Optional[Union[float, Decimal]] = None,
    ) -> AsyncIterator[Task[D, R]]:

        kwargs: Dict[str, Any] = {"payload": payload}
        if max_workers:
            kwargs["max_workers"] = max_workers
        if timeout:
            kwargs["timeout"] = timeout
        kwargs["budget"] = budget if budget is not None else self._budget_amount

        async with Executor(_engine=self, **kwargs) as executor:
            async for t in executor.submit(worker, data):
                yield t


class Job:
    """Functionality related to a single job."""

    def __init__(
        self,
        engine: Golem,
        expiration_time: datetime,
        payload: Payload,
    ):
        self.id = str(uuid.uuid4())
        self.engine = engine
        self.offers_collected: int = 0
        self.proposals_confirmed: int = 0
        self.expiration_time: datetime = expiration_time
        self.payload: Payload = payload

        self.agreements_pool = AgreementsPool(self.engine.emit)
        self.finished = asyncio.Event()

    async def _handle_proposal(
        self,
        proposal: OfferProposal,
        demand_builder: DemandBuilder,
    ) -> events.Event:
        """Handle a single `OfferProposal`.

        A `proposal` is scored and then can be rejected, responded with
        a counter-proposal or stored in an agreements pool to be used
        for negotiating an agreement.
        """

        async def reject_proposal(reason: str) -> events.ProposalRejected:
            """Reject `proposal` due to given `reason`."""
            await proposal.reject(reason)
            return events.ProposalRejected(prop_id=proposal.id, reason=reason)

        score = await self.engine._strategy.score_offer(proposal, self.agreements_pool)
        logger.debug(
            "Scored offer %s, provider: %s, strategy: %s, score: %f",
            proposal.id,
            proposal.props.get("golem.node.id.name"),
            type(self.engine._strategy).__name__,
            score,
        )

        if score < SCORE_NEUTRAL:
            return await reject_proposal("Score too low")

        if not proposal.is_draft:
            # Proposal is not yet a draft of an agreement

            # Check if any of the supported payment platforms matches the proposal
            common_platforms = self._get_common_payment_platforms(proposal)
            if common_platforms:
                demand_builder.properties["golem.com.payment.chosen-platform"] = next(
                    iter(common_platforms)
                )
            else:
                # reject proposal if there are no common payment platforms
                return await reject_proposal("No common payment platform")

            # Check if the timeout for debit note acceptance is not too low
            timeout = proposal.props.get(DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP)
            if timeout:
                if timeout < DEBIT_NOTE_MIN_TIMEOUT:
                    return await reject_proposal("Debit note acceptance timeout is too short")
                else:
                    demand_builder.properties[DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP] = timeout

            await proposal.respond(demand_builder.properties, demand_builder.constraints)
            return events.ProposalResponded(prop_id=proposal.id)

        else:
            # It's a draft agreement
            await self.agreements_pool.add_proposal(score, proposal)
            return events.ProposalConfirmed(prop_id=proposal.id)

    async def _find_offers_for_subscription(
        self,
        subscription: Subscription,
        demand_builder: DemandBuilder,
    ) -> None:
        """Create a market subscription and repeatedly collect offer proposals for it.

        Collected proposals are processed concurrently using a bounded number
        of asyncio tasks.

        :param state: A state related to a call to `Executor.submit()`
        """
        max_number_of_tasks = 5

        try:
            proposals = subscription.events()
        except Exception as ex:
            self.engine.emit(events.CollectFailed(sub_id=subscription.id, reason=str(ex)))
            raise

        # A semaphore is used to limit the number of handler tasks
        semaphore = asyncio.Semaphore(max_number_of_tasks)

        async for proposal in proposals:

            self.engine.emit(
                events.ProposalReceived(prop_id=proposal.id, provider_id=proposal.issuer)
            )
            self.offers_collected += 1

            async def handler(proposal_):
                """A coroutine that wraps `_handle_proposal()` method with error handling."""
                try:
                    event = await self._handle_proposal(proposal_, demand_builder)
                    assert isinstance(event, events.ProposalEvent)
                    self.engine.emit(event)
                    if isinstance(event, events.ProposalConfirmed):
                        self.proposals_confirmed += 1
                except CancelledError:
                    raise
                except Exception:
                    with contextlib.suppress(Exception):
                        self.engine.emit(
                            events.ProposalFailed(
                                prop_id=proposal_.id, exc_info=sys.exc_info()  # type: ignore
                            )
                        )
                finally:
                    semaphore.release()

            # Create a new handler task
            await semaphore.acquire()
            asyncio.get_event_loop().create_task(handler(proposal))

    async def find_offers(self) -> None:
        """Create demand subscription and process offers.
        When the subscription expires, create a new one. And so on...
        """

        builder = await self.engine.create_demand_builder(self.expiration_time, self.payload)

        while True:
            try:
                subscription = await builder.subscribe(self.engine._market_api)
                self.engine.emit(events.SubscriptionCreated(sub_id=subscription.id))
            except Exception as ex:
                self.engine.emit(events.SubscriptionFailed(reason=str(ex)))
                raise
            async with subscription:
                await self._find_offers_for_subscription(subscription, builder)

    # TODO: move to Golem
    def _get_common_payment_platforms(self, proposal: rest.market.OfferProposal) -> Set[str]:
        prov_platforms = {
            property.split(".")[4]
            for property in proposal.props
            if property.startswith("golem.com.payment.platform.") and property is not None
        }
        if not prov_platforms:
            prov_platforms = {"NGNT"}
        req_platforms = {
            allocation.payment_platform
            for allocation in self.engine._budget_allocations
            if allocation.payment_platform is not None
        }
        return req_platforms.intersection(prov_platforms)


DEFAULT_GET_OFFERS_TIMEOUT = timedelta(seconds=20)


class Executor(AsyncContextManager):
    """
    Task executor.

    Used to run batch tasks using the specified application package within providers' execution units.
    """

    @overload
    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        payload: Optional[Payload] = None,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        _engine: Golem,
    ):
        # A variant with explicit `_engine`
        ...

    @overload
    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        payload: Optional[Payload] = None,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
    ):
        # Standalone usage, with `payload` parameter
        ...

    @overload
    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        package: Optional[Payload] = None,
    ):
        # Standalone usage, with `package` parameter
        ...

    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        package: Optional[Payload] = None,
        payload: Optional[Payload] = None,
        _engine: Optional[Golem] = None,
    ):
        """Create a new executor.

        :param budget: [DEPRECATED use `Golem` instead] maximum budget for payments
        :param strategy: [DEPRECATED use `Golem` instead] market strategy used to
            select providers from the market (e.g. LeastExpensiveLinearPayuMS or DummyMS)
        :param subnet_tag: [DEPRECATED use `Golem` instead] use only providers in the
            subnet with the subnet_tag name
        :param driver: [DEPRECATED use `Golem` instead] name of the payment driver
            to use or `None` to use the default driver;
            only payment platforms with the specified driver will be used
        :param network: [DEPRECATED use `Golem` instead] name of the network
            to use or `None` to use the default network;
            only payment platforms with the specified network will be used
        :param event_consumer: [DEPRECATED use `Golem` instead] a callable that
            processes events related to the computation;
            by default it is a function that logs all events
        :param stream_output: [DEPRECATED use `Golem` instead]
            stream computation output from providers

        :param max_workers: maximum number of workers performing the computation
        :param timeout: timeout for the whole computation
        :param package: a package common for all tasks; vm.repo() function may be used
            to return package from a repository
        """
        logger.debug("Creating Executor instance; parameters: %s", locals())
        self.__standalone = False

        if _engine:
            self._engine = _engine
        else:
            warnings.warn(
                "stand-alone usage is deprecated, please use `Golem.execute_task` class instead.",
                DeprecationWarning,
            )
            self._engine = Golem(
                budget=budget,
                strategy=strategy,
                subnet_tag=subnet_tag,
                driver=driver,
                network=network,
                event_consumer=event_consumer,
                stream_output=stream_output,
            )
            self.__standalone = True

        if package:
            if payload:
                raise ValueError("Cannot use `payload` and `package` at the same time")
            logger.warning(
                f"`package` argument to `{self.__class__}` is deprecated, please use `payload` instead"
            )
            payload = package
        if not payload:
            raise ValueError("Executor `payload` must be specified")

        self._payload = payload
        self._timeout: timedelta = timeout
        self._max_workers = max_workers
        self._stack = AsyncExitStack()

    @property
    def driver(self) -> str:
        return self._engine.driver

    @property
    def network(self) -> str:
        return self._engine.network

    async def __aenter__(self) -> "Executor":
        if self.__standalone:
            await self._stack.enter_async_context(self._engine)
        self._expires = datetime.now(timezone.utc) + self._timeout
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._stack.aclose()

    def emit(self, event: events.Event) -> None:
        self._engine.emit(event)

    async def submit(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[Work, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
    ) -> AsyncIterator[Task[D, R]]:
        """Submit a computation to be executed on providers.

        :param worker: a callable that takes a WorkContext object and a list o tasks,
                       adds commands to the context object and yields committed commands
        :param data: an iterator of Task objects to be computed on providers
        :return: yields computation progress events
        """

        job = Job(self._engine, expiration_time=self._expires, payload=self._payload)
        self._engine.add_job(job)

        services: Set[asyncio.Task] = set()
        workers: Set[asyncio.Task] = set()

        try:
            generator = self._submit(worker, data, services, workers, job)
            try:
                async for result in generator:
                    yield result
            except GeneratorExit:
                logger.debug("Early exit from submit(), cancelling the computation")
                try:
                    # Cancel `generator`. It should then exit by raising `StopAsyncIteration`.
                    await generator.athrow(CancelledError)
                except StopAsyncIteration:
                    pass
        finally:
            # Cancel and gather all tasks to make sure all exceptions are retrieved.
            all_tasks = workers.union(services)
            for task in all_tasks:
                if not task.done():
                    logger.debug("Cancelling task: %s", task)
                    task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            # Signal that this computation is finished
            self._engine.finalize_job(job)

    async def _submit(
        self,
        worker: Callable[
            [WorkContext, AsyncIterator[Task[D, R]]],
            AsyncGenerator[Work, Awaitable[List[events.CommandEvent]]],
        ],
        data: Union[AsyncIterator[Task[D, R]], Iterable[Task[D, R]]],
        services: Set[asyncio.Task],
        workers: Set[asyncio.Task],
        job: Job,
    ) -> AsyncGenerator[Task[D, R], None]:

        self.emit(events.ComputationStarted(job.id, self._expires))

        done_queue: asyncio.Queue[Task[D, R]] = asyncio.Queue()

        def on_task_done(task: Task[D, R], status: TaskStatus) -> None:
            """Callback run when `task` is accepted or rejected."""
            if status == TaskStatus.ACCEPTED:
                done_queue.put_nowait(task)

        async def input_tasks() -> AsyncIterator[Task[D, R]]:
            if isinstance(data, AsyncIterator):
                async for task in data:
                    task._add_callback(on_task_done)
                    yield task
            else:
                for task in data:
                    task._add_callback(on_task_done)
                    yield task

        work_queue = SmartQueue(input_tasks())

        last_wid = 0

        async def start_worker(agreement: rest.market.Agreement, node_info: NodeInfo) -> None:

            nonlocal last_wid
            wid = last_wid
            last_wid += 1

            self.emit(events.WorkerStarted(agr_id=agreement.id))

            try:
                act = await self._engine.create_activity(agreement.id)
            except Exception:
                self.emit(
                    events.ActivityCreateFailed(
                        agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                    )
                )
                self.emit(events.WorkerFinished(agr_id=agreement.id))
                raise

            async with act:

                self.emit(events.ActivityCreated(act_id=act.id, agr_id=agreement.id))
                self._engine.approve_agreement_payments(agreement.id)
                work_context = WorkContext(
                    f"worker-{wid}", node_info, self._engine.storage_manager, emitter=self.emit
                )

                with work_queue.new_consumer() as consumer:
                    try:

                        async def task_generator() -> AsyncIterator[Task[D, R]]:
                            async for handle in consumer:
                                task = Task.for_handle(handle, work_queue, self.emit)
                                self._engine.emit(
                                    events.TaskStarted(
                                        agr_id=agreement.id, task_id=task.id, task_data=task.data
                                    )
                                )
                                yield task
                                self._engine.emit(
                                    events.TaskFinished(agr_id=agreement.id, task_id=task.id)
                                )

                        batch_generator = worker(work_context, task_generator())
                        try:
                            await self._engine.process_batches(agreement.id, act, batch_generator)
                        except StopAsyncIteration:
                            pass
                        self.emit(events.WorkerFinished(agr_id=agreement.id))
                    except Exception:
                        self.emit(
                            events.WorkerFinished(
                                agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                            )
                        )
                        raise
                    finally:
                        await self._engine.accept_payment_for_agreement(agreement.id)

        async def worker_starter() -> None:
            while True:
                await asyncio.sleep(2)
                await job.agreements_pool.cycle()
                if len(workers) < self._max_workers and await work_queue.has_unassigned_items():
                    new_task = None
                    try:
                        new_task = await job.agreements_pool.use_agreement(
                            lambda agreement, node: loop.create_task(start_worker(agreement, node))
                        )
                        if new_task is None:
                            continue
                        workers.add(new_task)
                    except CancelledError:
                        raise
                    except Exception:
                        if new_task:
                            new_task.cancel()
                        logger.debug("There was a problem during use_agreement", exc_info=True)

        loop = asyncio.get_event_loop()
        find_offers_task = loop.create_task(job.find_offers())

        wait_until_done = loop.create_task(work_queue.wait_until_done())
        worker_starter_task = loop.create_task(worker_starter())

        # Py38: find_offers_task.set_name('find_offers_task')

        get_offers_deadline = datetime.now(timezone.utc) + DEFAULT_GET_OFFERS_TIMEOUT
        get_done_task: Optional[asyncio.Task] = None
        services.update(
            {
                find_offers_task,
                wait_until_done,
                worker_starter_task,
            }
        )
        cancelled = False

        try:
            while wait_until_done in services or not done_queue.empty():

                now = datetime.now(timezone.utc)
                if now > self._expires:
                    raise TimeoutError(f"Computation timed out after {self._timeout}")
                if now > get_offers_deadline and job.proposals_confirmed == 0:
                    self.emit(
                        events.NoProposalsConfirmed(
                            num_offers=job.offers_collected, timeout=DEFAULT_GET_OFFERS_TIMEOUT
                        )
                    )
                    get_offers_deadline += DEFAULT_GET_OFFERS_TIMEOUT

                if not get_done_task:
                    get_done_task = loop.create_task(done_queue.get())
                    services.add(get_done_task)

                done, pending = await asyncio.wait(
                    services.union(workers), timeout=10, return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    # if an exception occurred when a service task was running
                    if task in services and not task.cancelled() and task.exception():
                        raise cast(BaseException, task.exception())
                    if task in workers:
                        try:
                            await task
                        except Exception:
                            pass

                workers -= done
                services -= done

                assert get_done_task
                if get_done_task.done():
                    yield get_done_task.result()
                    assert get_done_task not in services
                    get_done_task = None

            self.emit(events.ComputationFinished(job.id))

        except (Exception, CancelledError, KeyboardInterrupt):
            self.emit(events.ComputationFinished(job.id, exc_info=sys.exc_info()))  # type: ignore
            cancelled = True

        finally:
            # Importing this at the beginning would cause circular dependencies
            from ..log import pluralize

            for task in services:
                task.cancel()
            if cancelled:
                reason = {"message": "Work cancelled", "golem.requestor.code": "Cancelled"}
                for worker_task in workers:
                    worker_task.cancel()
            else:
                reason = {
                    "message": "Successfully finished all work",
                    "golem.requestor.code": "Success",
                }

            if workers:
                _, pending = await asyncio.wait(
                    workers, timeout=1, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.info("Waiting for %s to finish...", pluralize(len(pending), "worker"))
                    await asyncio.wait(workers, timeout=9, return_when=asyncio.ALL_COMPLETED)

            try:
                logger.debug("Terminating agreements...")
                await job.agreements_pool.terminate_all(reason=reason)
            except Exception:
                logger.debug("Problem with agreements termination", exc_info=True)

            try:
                logger.info("Waiting for Executor services to finish...")
                _, pending = await asyncio.wait(
                    workers.union(services), timeout=10, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.debug(
                        "%s still running: %s", pluralize(len(pending), "service"), pending
                    )
            except Exception:
                # TODO: add message
                logger.debug("TODO", exc_info=True)
