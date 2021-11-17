import asyncio
import aiohttp
from asyncio import CancelledError
from collections import defaultdict
import contextlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import itertools
import logging
import os
import sys
from typing import (
    AsyncContextManager,
    Awaitable,
    Callable,
    cast,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Union,
)
from typing_extensions import Final, AsyncGenerator

if sys.version_info >= (3, 7):
    from contextlib import AsyncExitStack
else:
    from async_exit_stack import AsyncExitStack  # type: ignore

from yapapi import rest, events
from yapapi.agreements_pool import AgreementsPool
from yapapi.ctx import WorkContext
from yapapi.payload import Payload
from yapapi import props
from yapapi.props import com
from yapapi.props.builder import DemandBuilder, DemandDecorator
from yapapi.rest.activity import CommandExecutionError, Activity
from yapapi.rest.market import Agreement, AgreementDetails, OfferProposal, Subscription
from yapapi.script import Script
from yapapi.script.command import BatchCommand
from yapapi.storage import gftp
from yapapi.strategy import (
    DecreaseScoreForUnconfirmedAgreement,
    LeastExpensiveLinearPayuMS,
    MarketStrategy,
    SCORE_NEUTRAL,
)
from yapapi.utils import AsyncWrapper


DEBIT_NOTE_MIN_TIMEOUT: Final[int] = 30  # in seconds
"Shortest debit note acceptance timeout the requestor will accept."

DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP: Final[str] = "golem.com.payment.debit-notes.accept-timeout?"

DEFAULT_DRIVER: str = os.getenv("YAGNA_PAYMENT_DRIVER", "erc20").lower()
DEFAULT_NETWORK: str = os.getenv("YAGNA_PAYMENT_NETWORK", "rinkeby").lower()
DEFAULT_SUBNET: Optional[str] = os.getenv("YAGNA_SUBNET", "devnet-beta")


logger = logging.getLogger("yapapi.executor")


class NoPaymentAccountError(Exception):
    """The error raised if no payment account for the required driver/network is available."""

    required_driver: str
    """Payment driver required for the account."""

    required_network: str
    """Network required for the account."""

    def __init__(self, required_driver: str, required_network: str):
        """Initialize `NoPaymentAccountError`.

        :param required_driver: payment driver for which initialization was required
        :param required_network: payment network for which initialization was required
        """
        self.required_driver: str = required_driver
        self.required_network: str = required_network

    def __str__(self) -> str:
        return (
            f"No payment account available for driver `{self.required_driver}`"
            f" and network `{self.required_network}`"
        )


# Type aliases to make some type annotations more meaningful
JobId = str
AgreementId = str


class _Engine:
    """Base execution engine containing functions common to all modes of operation."""

    def __init__(
        self,
        *,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        payment_driver: Optional[str] = None,
        payment_network: Optional[str] = None,
        event_consumer: Optional[Callable[[events.Event], None]] = None,
        stream_output: bool = False,
        app_key: Optional[str] = None,
    ):
        """Initialize the engine.

        :param budget: maximum budget for payments
        :param strategy: market strategy used to select providers from the market
            (e.g. LeastExpensiveLinearPayuMS or DummyMS)
        :param subnet_tag: use only providers in the subnet with the subnet_tag name.
            Uses `YAGNA_SUBNET` environment variable, defaults to `None`
        :param payment_driver: name of the payment driver to use. Uses `YAGNA_PAYMENT_DRIVER`
            environment variable, defaults to `erc20`. Only payment platforms with
            the specified driver will be used
        :param payment_network: name of the payment network to use. Uses `YAGNA_PAYMENT_NETWORK`
        environment variable, defaults to `rinkeby`. Only payment platforms with the specified
            network will be used
        :param event_consumer: a callable that processes events related to the
            computation; by default it is a function that logs all events
        :param stream_output: stream computation output from providers
        :param app_key: optional Yagna application key. If not provided, the default is to
                        get the value from `YAGNA_APPKEY` environment variable
        """
        self._api_config = rest.Configuration(app_key)
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

        self._subnet: Optional[str] = subnet_tag or DEFAULT_SUBNET
        self._payment_driver: str = payment_driver.lower() if payment_driver else DEFAULT_DRIVER
        self._payment_network: str = payment_network.lower() if payment_network else DEFAULT_NETWORK

        if not event_consumer:
            # Use local import to avoid cyclic imports when yapapi.log
            # is imported by client code
            from yapapi.log import log_event_repr, log_summary

            event_consumer = log_summary(log_event_repr)

        # Add buffering to the provided event emitter to make sure
        # that emitting events will not block
        self._wrapped_consumer = AsyncWrapper(event_consumer)

        self._stream_output = stream_output

        # a set of `Job` instances used to track jobs - computations or services - started
        # it can be used to wait until all jobs are finished
        self._jobs: Set[Job] = set()

        # initialize the payment structures
        self._agreements_to_pay: Dict[JobId, Set[AgreementId]] = defaultdict(set)
        self._agreements_accepting_debit_notes: Dict[JobId, Set[AgreementId]] = defaultdict(set)
        self._invoices: Dict[AgreementId, rest.payment.Invoice] = dict()
        self._payment_closing: bool = False

        self._process_invoices_job: Optional[asyncio.Task] = None

        # a set of async generators created by executors that use this engine
        self._generators: Set[AsyncGenerator] = set()
        self._services: Set[asyncio.Task] = set()
        self._stack = AsyncExitStack()

        self._started = False

    async def create_demand_builder(
        self, expiration_time: datetime, payload: Payload
    ) -> DemandBuilder:
        """Create a `DemandBuilder` for given `payload` and `expiration_time`."""
        builder = DemandBuilder()
        builder.add(props.Activity(expiration=expiration_time, multi_activity=True))
        builder.add(props.NodeInfo(subnet_tag=self._subnet))
        if self._subnet:
            builder.ensure(f"({props.NodeInfoKeys.subnet_tag}={self._subnet})")
        await builder.decorate(self.payment_decorator, self.strategy, payload)
        return builder

    @property
    def payment_driver(self) -> str:
        """Return the name of the payment driver used by this engine."""
        return self._payment_driver

    @property
    def payment_network(self) -> str:
        """Return the name of the payment payment network used by this engine."""
        return self._payment_network

    @property
    def storage_manager(self):
        """Return the storage manager used by this engine."""
        return self._storage_manager

    @property
    def strategy(self) -> MarketStrategy:
        """Return the instance of `MarketStrategy` used by this engine."""
        return self._strategy

    @property
    def subnet_tag(self) -> Optional[str]:
        """Return the name of the subnet used by this engine, or `None` if it is not set."""
        return self._subnet

    @property
    def started(self) -> bool:
        """Return `True` if this instance is initialized, `False` otherwise."""
        return self._started

    def emit(self, event: events.Event) -> None:
        """Emit an event to be consumed by this engine's event consumer."""
        if self._wrapped_consumer:
            self._wrapped_consumer.async_call(event)

    async def stop(self, *exc_info) -> Optional[bool]:
        """Stop the engine.

        This *must* be called at the end of the work, by the Engine user.
        """
        if exc_info[0] is not None:
            self.emit(events.ExecutionInterrupted(exc_info))  # type: ignore
        return await self._stack.__aexit__(None, None, None)

    async def start(self):
        """Start the engine.

        This is supposed to be called exactly once. Repeated or interrupted call
        will leave the engine in an unrecoverable state.
        """

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

        net_client = await stack.enter_async_context(self._api_config.net())
        self._net_api = rest.Net(net_client)

        # TODO replace with a proper REST API client once ya-client and ya-aioclient are updated
        # https://github.com/golemfactory/yapapi/issues/636
        self._root_api_session = await stack.enter_async_context(
            aiohttp.ClientSession(
                headers=net_client.default_headers,
            )
        )

        self.payment_decorator = _Engine.PaymentDecorator(await self._create_allocations())

        # TODO: make the method starting the process_invoices() task an async context manager
        # to simplify code in __aexit__()
        loop = asyncio.get_event_loop()
        self._process_invoices_job = loop.create_task(self._process_invoices())
        self._services.add(self._process_invoices_job)
        self._services.add(loop.create_task(self._process_debit_notes()))

        self._storage_manager = await stack.enter_async_context(gftp.provider())

        stack.push_async_exit(self._shutdown)

        self._started = True

    async def add_to_async_context(self, async_context_manager: AsyncContextManager) -> None:
        await self._stack.enter_async_context(async_context_manager)

    def _unpaid_agreement_ids(self) -> Set[AgreementId]:
        """Return the set of all yet unpaid agreement ids."""

        unpaid_agreement_ids = set()
        for job_id, agreement_ids in self._agreements_to_pay.items():
            unpaid_agreement_ids.update(agreement_ids)
        return unpaid_agreement_ids

    async def _shutdown(self, *exc_info):
        """Shutdown this Golem instance."""

        # Importing this at the beginning would cause circular dependencies
        from yapapi.log import pluralize

        logger.info("Golem is shutting down...")

        # Some generators created by `execute_tasks` may still have elements;
        # if we don't close them now, their jobs will never be marked as finished.
        for gen in self._generators:
            await gen.aclose()

        # Wait until all computations are finished
        logger.debug("Waiting for the jobs to finish...")
        await asyncio.gather(*[job.finished.wait() for job in self._jobs])
        logger.info("All jobs have finished")

        self._payment_closing = True

        # Cancel all services except the one that processes invoices
        for task in self._services:
            if task is not self._process_invoices_job:
                task.cancel()

        # Wait for some time for invoices for unpaid agreements,
        # then cancel the invoices service
        if self._process_invoices_job:

            unpaid_agreements = self._unpaid_agreement_ids()
            if unpaid_agreements:
                logger.info(
                    "%s still unpaid, waiting for invoices...",
                    pluralize(len(unpaid_agreements), "agreement"),
                )
                try:
                    await asyncio.wait_for(self._process_invoices_job, timeout=30)
                except asyncio.TimeoutError:
                    logger.debug("process_invoices_job cancelled")
                unpaid_agreements = self._unpaid_agreement_ids()
                if unpaid_agreements:
                    logger.warning("Unpaid agreements: %s", unpaid_agreements)

            self._process_invoices_job.cancel()

        try:
            logger.info("Waiting for Golem services to finish...")
            _, pending = await asyncio.wait(
                self._services, timeout=10, return_when=asyncio.ALL_COMPLETED
            )
            if pending:
                logger.debug("%s still running: %s", pluralize(len(pending), "service"), pending)
        except Exception:
            logger.debug("Got error when waiting for services to finish", exc_info=True)

    async def _create_allocations(self) -> rest.payment.MarketDecoration:

        if not self._budget_allocations:
            async for account in self._payment_api.accounts():
                driver = account.driver.lower()
                network = account.network.lower()
                if (driver, network) != (self._payment_driver, self._payment_network):
                    logger.debug(
                        "Not using payment platform `%s`, platform's driver/network "
                        "`%s`/`%s` is different than requested driver/network `%s`/`%s`",
                        account.platform,
                        driver,
                        network,
                        self._payment_driver,
                        self._payment_network,
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
                raise NoPaymentAccountError(self._payment_driver, self._payment_network)

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

    async def _process_invoices(self) -> None:
        """Process incoming invoices."""

        async for invoice in self._payment_api.incoming_invoices():
            job_id = next(
                (
                    id
                    for id in self._agreements_to_pay
                    if invoice.agreement_id in self._agreements_to_pay[id]
                ),
                None,
            )
            if job_id is not None:
                self.emit(
                    events.InvoiceReceived(
                        job_id=job_id,
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
                            job_id=job_id,
                            agr_id=invoice.agreement_id,
                            exc_info=sys.exc_info(),  # type: ignore
                        )
                    )
                else:
                    self._agreements_to_pay[job_id].remove(invoice.agreement_id)
                    assert invoice.agreement_id in self._agreements_accepting_debit_notes[job_id]
                    self._agreements_accepting_debit_notes[job_id].remove(invoice.agreement_id)
                    self.emit(
                        events.PaymentAccepted(
                            job_id=job_id,
                            agr_id=invoice.agreement_id,
                            inv_id=invoice.invoice_id,
                            amount=invoice.amount,
                        )
                    )
            else:
                self._invoices[invoice.agreement_id] = invoice
            if self._payment_closing and not any(
                agr_ids for agr_ids in self._agreements_to_pay.values()
            ):
                break

    async def _process_debit_notes(self) -> None:
        """Process incoming debit notes."""

        async for debit_note in self._payment_api.incoming_debit_notes():
            job_id = next(
                (
                    id
                    for id in self._agreements_accepting_debit_notes
                    if debit_note.agreement_id in self._agreements_accepting_debit_notes[id]
                ),
                None,
            )
            if job_id is not None:
                self.emit(
                    events.DebitNoteReceived(
                        job_id=job_id,
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
                            job_id=job_id, agr_id=debit_note.agreement_id, exc_info=sys.exc_info()  # type: ignore
                        )
                    )
            if self._payment_closing and not self._agreements_to_pay:
                break

    async def accept_payments_for_agreement(
        self, job_id: str, agreement_id: str, *, partial: bool = False
    ) -> None:
        """Add given agreement to the set of agreements for which invoices should be accepted."""

        self.emit(events.PaymentPrepared(job_id=job_id, agr_id=agreement_id))
        inv = self._invoices.get(agreement_id)
        if inv is None:
            self._agreements_to_pay[job_id].add(agreement_id)
            self.emit(events.PaymentQueued(job_id=job_id, agr_id=agreement_id))
            return
        del self._invoices[agreement_id]
        allocation = self._get_allocation(inv)
        await inv.accept(amount=inv.amount, allocation=allocation)
        self.emit(
            events.PaymentAccepted(
                job_id=job_id, agr_id=agreement_id, inv_id=inv.invoice_id, amount=inv.amount
            )
        )

    def accept_debit_notes_for_agreement(self, job_id: str, agreement_id: str) -> None:
        """Add given agreement to the set of agreements for which debit notes should be accepted."""
        self._agreements_accepting_debit_notes[job_id].add(agreement_id)

    def add_job(self, job: "Job"):
        """Register a job with this engine."""
        self._jobs.add(job)

    @staticmethod
    def finalize_job(job: "Job"):
        """Mark a job as finished."""
        job.finished.set()

    def register_generator(self, generator: AsyncGenerator) -> None:
        """Register a generator with this engine."""
        self._generators.add(generator)

    @dataclass
    class PaymentDecorator(DemandDecorator):
        """A `DemandDecorator` that adds payment-related constraints and properties to a Demand."""

        market_decoration: rest.payment.MarketDecoration

        async def decorate_demand(self, demand: DemandBuilder):
            """Add properties and constraints to a Demand."""
            for constraint in self.market_decoration.constraints:
                demand.ensure(constraint)
            demand.properties.update({p.key: p.value for p in self.market_decoration.properties})

    async def create_activity(self, agreement_id: str) -> Activity:
        """Create an activity for given `agreement_id`."""
        return await self._activity_api.new_activity(
            agreement_id, stream_events=self._stream_output
        )

    async def start_worker(
        self, job: "Job", run_worker: Callable[[Agreement, Activity, WorkContext], Awaitable]
    ) -> Optional[asyncio.Task]:
        loop = asyncio.get_event_loop()

        async def worker_task(agreement: Agreement, agreement_details: AgreementDetails):
            """A coroutine run by every worker task.

            It creates an Activity and WorkContext for given Agreement
            and then passes them to `run_worker`.
            """

            self.emit(events.WorkerStarted(job_id=job.id, agr_id=agreement.id))

            try:
                activity = await self.create_activity(agreement.id)
                self.emit(
                    events.ActivityCreated(job_id=job.id, act_id=activity.id, agr_id=agreement.id)
                )
            except Exception:
                self.emit(
                    events.ActivityCreateFailed(
                        job_id=job.id, agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                    )
                )
                raise

            async with activity:
                self.accept_debit_notes_for_agreement(job.id, agreement.id)
                work_context = WorkContext(
                    activity, agreement_details, self.storage_manager, emitter=self.emit
                )
                await run_worker(agreement, activity, work_context)

        return await job.agreements_pool.use_agreement(
            lambda agreement, details: loop.create_task(worker_task(agreement, details))
        )

    async def process_batches(
        self,
        job_id: str,
        agreement_id: str,
        activity: rest.activity.Activity,
        batch_generator: AsyncGenerator[Script, Awaitable[List[events.CommandEvent]]],
    ) -> None:
        """Send command batches produced by `batch_generator` to `activity`."""

        script: Script = await batch_generator.__anext__()

        while True:
            script_id = str(script.id)

            batch_deadline = (
                datetime.now(timezone.utc) + script.timeout if script.timeout is not None else None
            )

            try:
                await script._before()
                batch: List[BatchCommand] = script._evaluate()
                remote = await activity.send(batch, deadline=batch_deadline)
            except Exception:
                script = await batch_generator.athrow(*sys.exc_info())
                continue

            self.emit(
                events.ScriptSent(
                    job_id=job_id, agr_id=agreement_id, script_id=script_id, cmds=batch
                )
            )

            async def get_batch_results() -> List[events.CommandEvent]:
                results = []
                async for evt_ctx in remote:
                    evt = evt_ctx.event(
                        job_id=job_id, agr_id=agreement_id, script_id=script_id, cmds=batch
                    )
                    self.emit(evt)
                    results.append(evt)
                    if isinstance(evt, events.CommandExecuted):
                        if evt.success:
                            script._set_cmd_result(evt)
                        else:
                            raise CommandExecutionError(evt.command, evt.message, evt.stderr)

                self.emit(
                    events.GettingResults(job_id=job_id, agr_id=agreement_id, script_id=script_id)
                )
                await script._after()
                self.emit(
                    events.ScriptFinished(job_id=job_id, agr_id=agreement_id, script_id=script_id)
                )
                await self.accept_payments_for_agreement(job_id, agreement_id, partial=True)
                return results

            loop = asyncio.get_event_loop()

            if script.wait_for_results:
                # Block until the results are available
                try:
                    future_results = loop.create_future()
                    results = await get_batch_results()
                    future_results.set_result(results)
                except Exception:
                    # Raise the exception in `batch_generator` (the `worker` coroutine).
                    # If the client code is able to handle it then we'll proceed with
                    # subsequent batches. Otherwise the worker finishes with error.
                    script = await batch_generator.athrow(*sys.exc_info())
                else:
                    script = await batch_generator.asend(future_results)

            else:
                # Schedule the coroutine in a separate asyncio task
                future_results = loop.create_task(get_batch_results())
                script = await batch_generator.asend(future_results)


class Job:
    """Functionality related to a single job.

    Responsible for posting a Demand to market and collecting Offer proposals for the Demand.
    """

    # Infinite supply of generated job ids: "1", "2", ...
    # We prefer short ids since they would make log messages more readable.
    _generated_job_ids: Iterator[str] = (str(n) for n in itertools.count(1))

    # Job ids already used, tracked to avoid duplicates
    _used_job_ids: Set[str] = set()

    def __init__(
        self,
        engine: _Engine,
        expiration_time: datetime,
        payload: Payload,
        id: Optional[str] = None,
    ):
        """Initialize a `Job` instance.

        param engine: a `Golem` engine which will run this job
        param expiration_time: expiration time for the job; all agreements created for this job
            must expire before this date
        param payload: definition of a service runtime or a runtime package that needs to
            be deployed on providers for executing this job
        param id: an optional string to be used to identify this job in events emitted
            by the engine. ValueError is raised if a job with the same id has already been created.
            If not specified, a unique identifier will be generated.
        """

        if id:
            if id in Job._used_job_ids:
                raise ValueError(f"Non unique job id {id}")
            self.id = id
        else:
            self.id = next(id for id in Job._generated_job_ids if id not in Job._used_job_ids)
        Job._used_job_ids.add(self.id)

        self.engine = engine
        self.offers_collected: int = 0
        self.proposals_confirmed: int = 0
        self.expiration_time: datetime = expiration_time
        self.payload: Payload = payload

        self.agreements_pool = AgreementsPool(self.id, self.engine.emit)
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
            return events.ProposalRejected(job_id=self.id, prop_id=proposal.id, reason=reason)

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
            return events.ProposalResponded(job_id=self.id, prop_id=proposal.id)

        else:
            # It's a draft agreement
            await self.agreements_pool.add_proposal(score, proposal)
            return events.ProposalConfirmed(job_id=self.id, prop_id=proposal.id)

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
                events.ProposalReceived(
                    job_id=self.id, prop_id=proposal.id, provider_id=proposal.issuer
                )
            )
            self.offers_collected += 1

            async def handler(proposal_):
                """Wrap `_handle_proposal()` method with error handling."""
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
                                job_id=self.id, prop_id=proposal_.id, exc_info=sys.exc_info()  # type: ignore
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
                self.engine.emit(events.SubscriptionCreated(job_id=self.id, sub_id=subscription.id))
            except Exception as ex:
                self.engine.emit(events.SubscriptionFailed(job_id=self.id, reason=str(ex)))
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
