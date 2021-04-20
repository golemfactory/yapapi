"""
An implementation of the new Golem's task executor.
"""
import asyncio
from asyncio import CancelledError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import os
import sys
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
    cast,
)
import traceback


from dataclasses import dataclass
from yapapi.executor.agreements_pool import AgreementsPool
from typing_extensions import Final, AsyncGenerator

from .ctx import CaptureContext, CommandContainer, Work, WorkContext
from .events import Event
from . import events
from .task import Task, TaskStatus
from .utils import AsyncWrapper
from ..package import Package
from ..props import Activity, com, NodeInfo, NodeInfoKeys
from ..props.builder import DemandBuilder
from .. import rest
from ..rest.activity import CommandExecutionError
from ..rest.market import Subscription
from ..storage import gftp
from ._smartq import SmartQueue, Handle
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


@dataclass
class _ExecutorConfig:
    max_workers: int = 5
    timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT
    get_offers_timeout: timedelta = timedelta(seconds=20)
    traceback: bool = bool(os.getenv("YAPAPI_TRACEBACK", 0))


D = TypeVar("D")  # Type var for task data
R = TypeVar("R")  # Type var for task result


class Executor(AsyncContextManager):
    """
    Task executor.

    Used to run tasks using the specified application package within providers' execution units.
    """

    def __init__(
        self,
        *,
        package: Package,
        max_workers: int = 5,
        timeout: timedelta = DEFAULT_EXECUTOR_TIMEOUT,
        budget: Union[float, Decimal],
        strategy: Optional[MarketStrategy] = None,
        subnet_tag: Optional[str] = None,
        driver: Optional[str] = None,
        network: Optional[str] = None,
        event_consumer: Optional[Callable[[Event], None]] = None,
        stream_output: bool = False,
    ):
        """Create a new executor.

        :param package: a package common for all tasks; vm.repo() function may be
            used to return package from a repository
        :param max_workers: maximum number of workers doing the computation
        :param timeout: timeout for the whole computation
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
        """
        logger.debug("Creating Executor instance; parameters: %s", locals())

        self._subnet: Optional[str] = subnet_tag
        self._driver = driver.lower() if driver else DEFAULT_DRIVER
        self._network = network.lower() if network else DEFAULT_NETWORK
        self._stream_output = stream_output
        if not strategy:
            strategy = LeastExpensiveLinearPayuMS(
                max_fixed_price=Decimal("1.0"),
                max_price_for={com.Counter.CPU: Decimal("0.2"), com.Counter.TIME: Decimal("0.1")},
            )
            # The factor 0.5 below means that an offer for a provider that failed to confirm
            # the last agreement proposed to them will have it's score multiplied by 0.5.
            strategy = DecreaseScoreForUnconfirmedAgreement(strategy, 0.5)
        self._strategy = strategy
        self._api_config = rest.Configuration()
        self._stack = AsyncExitStack()
        self._package = package
        self._conf = _ExecutorConfig(max_workers, timeout)
        # TODO: setup precision
        self._budget_amount = Decimal(budget)
        self._budget_allocations: List[rest.payment.Allocation] = []

        if not event_consumer:
            # Use local import to avoid cyclic imports when yapapi.log
            # is imported by client code
            from ..log import log_event_repr

            event_consumer = log_event_repr

        # Add buffering to the provided event emitter to make sure
        # that emitting events will not block
        self._wrapped_consumer = AsyncWrapper(event_consumer)
        # Each call to `submit()` will add an instance of `asyncio.Event` to this set.
        # This event can be used to wait until the call to `submit()` is finished.
        self._active_computations: Set[asyncio.Event] = set()

    @property
    def driver(self) -> str:
        return self._driver

    @property
    def network(self) -> str:
        return self._network

    @property
    def strategy(self) -> MarketStrategy:
        return self._strategy

    async def submit(
        self,
        worker: Callable[[WorkContext, AsyncIterator[Task[D, R]]], AsyncGenerator[Work, None]],
        data: Iterable[Task[D, R]],
    ) -> AsyncIterator[Task[D, R]]:
        """Submit a computation to be executed on providers.

        :param worker: a callable that takes a WorkContext object and a list o tasks,
                       adds commands to the context object and yields committed commands
        :param data: an iterator of Task objects to be computed on providers
        :return: yields computation progress events
        """

        computation_finished = asyncio.Event()
        self._active_computations.add(computation_finished)

        services: Set[asyncio.Task] = set()
        workers: Set[asyncio.Task] = set()

        try:
            generator = self._submit(worker, data, services, workers)
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
            computation_finished.set()
            self._active_computations.remove(computation_finished)

    async def _submit(
        self,
        worker: Callable[[WorkContext, AsyncIterator[Task[D, R]]], AsyncGenerator[Work, None]],
        data: Iterable[Task[D, R]],
        services: Set[asyncio.Task],
        workers: Set[asyncio.Task],
    ) -> AsyncGenerator[Task[D, R], None]:

        emit = cast(Callable[[Event], None], self._wrapped_consumer.async_call)

        multi_payment_decoration = await self._create_allocations()

        emit(events.ComputationStarted(self._expires))

        # Building offer
        builder = DemandBuilder()
        builder.add(Activity(expiration=self._expires, multi_activity=True))
        builder.add(NodeInfo(subnet_tag=self._subnet))
        if self._subnet:
            builder.ensure(f"({NodeInfoKeys.subnet_tag}={self._subnet})")
        for constraint in multi_payment_decoration.constraints:
            builder.ensure(constraint)
        builder.properties.update({p.key: p.value for p in multi_payment_decoration.properties})
        await self._package.decorate_demand(builder)
        await self._strategy.decorate_demand(builder)

        agreements_pool = AgreementsPool(emit)
        market_api = self._market_api
        activity_api: rest.Activity = self._activity_api
        strategy = self._strategy

        done_queue: asyncio.Queue[Task[D, R]] = asyncio.Queue()
        stream_output = self._stream_output

        def on_task_done(task: Task[D, R], status: TaskStatus) -> None:
            """Callback run when `task` is accepted or rejected."""
            if status == TaskStatus.ACCEPTED:
                done_queue.put_nowait(task)

        def input_tasks() -> Iterable[Task[D, R]]:
            for task in data:
                task._add_callback(on_task_done)
                yield task

        work_queue = SmartQueue(input_tasks())

        last_wid = 0

        agreements_to_pay: Set[str] = set()
        agreements_accepting_debit_notes: Set[str] = set()
        invoices: Dict[str, rest.payment.Invoice] = dict()
        payment_closing: bool = False

        offers_collected = 0
        proposals_confirmed = 0

        async def process_invoices() -> None:
            async for invoice in self._payment_api.incoming_invoices():
                if invoice.agreement_id in agreements_to_pay:
                    emit(
                        events.InvoiceReceived(
                            agr_id=invoice.agreement_id,
                            inv_id=invoice.invoice_id,
                            amount=invoice.amount,
                        )
                    )
                    allocation = self._get_allocation(invoice)
                    try:
                        await invoice.accept(amount=invoice.amount, allocation=allocation)
                    except CancelledError:
                        raise
                    except Exception:
                        emit(
                            events.PaymentFailed(
                                agr_id=invoice.agreement_id, exc_info=sys.exc_info()  # type: ignore
                            )
                        )
                    else:
                        agreements_to_pay.remove(invoice.agreement_id)
                        assert invoice.agreement_id in agreements_accepting_debit_notes
                        agreements_accepting_debit_notes.remove(invoice.agreement_id)
                        emit(
                            events.PaymentAccepted(
                                agr_id=invoice.agreement_id,
                                inv_id=invoice.invoice_id,
                                amount=invoice.amount,
                            )
                        )
                else:
                    invoices[invoice.agreement_id] = invoice
                if payment_closing and not agreements_to_pay:
                    break

        # TODO Consider processing invoices and debit notes together
        async def process_debit_notes() -> None:
            async for debit_note in self._payment_api.incoming_debit_notes():
                if debit_note.agreement_id in agreements_accepting_debit_notes:
                    emit(
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
                        emit(
                            events.PaymentFailed(
                                agr_id=debit_note.agreement_id, exc_info=sys.exc_info()  # type: ignore
                            )
                        )
                if payment_closing and not agreements_to_pay:
                    break

        async def accept_payment_for_agreement(agreement_id: str, *, partial: bool = False) -> None:
            emit(events.PaymentPrepared(agr_id=agreement_id))
            inv = invoices.get(agreement_id)
            if inv is None:
                agreements_to_pay.add(agreement_id)
                emit(events.PaymentQueued(agr_id=agreement_id))
                return
            del invoices[agreement_id]
            allocation = self._get_allocation(inv)
            await inv.accept(amount=inv.amount, allocation=allocation)
            emit(
                events.PaymentAccepted(
                    agr_id=agreement_id, inv_id=inv.invoice_id, amount=inv.amount
                )
            )

        async def find_offers_for_subscription(subscription: Subscription) -> None:
            """Subscribe to offers and process them continuously."""

            async def reject_proposal(proposal, reason):
                await proposal.reject(reason=reason)
                emit(events.ProposalRejected(prop_id=proposal.id, reason=reason))

            nonlocal offers_collected, proposals_confirmed

            emit(events.SubscriptionCreated(sub_id=subscription.id))
            try:
                proposals = subscription.events()
            except Exception as ex:
                emit(events.CollectFailed(sub_id=subscription.id, reason=str(ex)))
                raise

            async for proposal in proposals:

                emit(events.ProposalReceived(prop_id=proposal.id, provider_id=proposal.issuer))
                offers_collected += 1
                try:
                    score = await strategy.score_offer(proposal, agreements_pool)
                    logger.debug(
                        "Scored offer %s, provider: %s, strategy: %s, score: %f",
                        proposal.id,
                        proposal.props.get("golem.node.id.name"),
                        type(strategy).__name__,
                        score,
                    )

                    if score < SCORE_NEUTRAL:
                        await reject_proposal(proposal, "Score too low")

                    elif not proposal.is_draft:
                        common_platforms = self._get_common_payment_platforms(proposal)
                        if common_platforms:
                            builder.properties["golem.com.payment.chosen-platform"] = next(
                                iter(common_platforms)
                            )
                        else:
                            # reject proposal if there are no common payment platforms
                            await reject_proposal(proposal, "No common payment platform")
                            continue
                        timeout = proposal.props.get(DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP)
                        if timeout:
                            if timeout < DEBIT_NOTE_MIN_TIMEOUT:
                                await reject_proposal(
                                    proposal, "Debit note acceptance timeout too short"
                                )
                                continue
                            else:
                                builder.properties[DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP] = timeout

                        await proposal.respond(builder.properties, builder.constraints)
                        emit(events.ProposalResponded(prop_id=proposal.id))

                    else:
                        emit(events.ProposalConfirmed(prop_id=proposal.id))
                        await agreements_pool.add_proposal(score, proposal)
                        proposals_confirmed += 1

                except CancelledError:
                    raise
                except Exception:
                    emit(
                        events.ProposalFailed(
                            prop_id=proposal.id, exc_info=sys.exc_info()  # type: ignore
                        )
                    )

        async def find_offers() -> None:
            """Create demand subscription and process offers.

            When the subscription expires, create a new one. And so on...
            """
            while True:
                try:
                    subscription = await builder.subscribe(market_api)
                except Exception as ex:
                    emit(events.SubscriptionFailed(reason=str(ex)))
                    raise
                async with subscription:
                    await find_offers_for_subscription(subscription)

        # aio_session = await self._stack.enter_async_context(aiohttp.ClientSession())
        # storage_manager = await DavStorageProvider.for_directory(
        #    aio_session,
        #    "http://127.0.0.1:8077/",
        #    "test1",
        #    auth=aiohttp.BasicAuth("alice", "secret1234"),
        # )
        storage_manager = await self._stack.enter_async_context(gftp.provider())

        async def start_worker(agreement: rest.market.Agreement, node_info: NodeInfo) -> None:
            nonlocal last_wid
            wid = last_wid
            last_wid += 1

            emit(events.WorkerStarted(agr_id=agreement.id))

            try:
                act = await activity_api.new_activity(agreement.id)
            except Exception:
                emit(
                    events.ActivityCreateFailed(
                        agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                    )
                )
                emit(events.WorkerFinished(agr_id=agreement.id))
                raise
            async with act:
                emit(events.ActivityCreated(act_id=act.id, agr_id=agreement.id))
                agreements_accepting_debit_notes.add(agreement.id)
                work_context = WorkContext(
                    f"worker-{wid}", node_info, storage_manager, emitter=emit
                )
                with work_queue.new_consumer() as consumer:

                    command_generator = worker(
                        work_context,
                        (Task.for_handle(handle, work_queue, emit) async for handle in consumer),
                    )
                    async for batch in command_generator:
                        batch_deadline = (
                            datetime.now(timezone.utc) + batch.timeout if batch.timeout else None
                        )
                        try:
                            current_worker_task = consumer.last_item
                            if current_worker_task:
                                emit(
                                    events.TaskStarted(
                                        agr_id=agreement.id,
                                        task_id=current_worker_task.id,
                                        task_data=current_worker_task.data,
                                    )
                                )
                            task_id = current_worker_task.id if current_worker_task else None
                            await batch.prepare()
                            cc = CommandContainer()
                            batch.register(cc)
                            remote = await act.send(
                                cc.commands(), stream_output, deadline=batch_deadline
                            )
                            cmds = cc.commands()
                            emit(events.ScriptSent(agr_id=agreement.id, task_id=task_id, cmds=cmds))

                            async for evt_ctx in remote:
                                evt = evt_ctx.event(agr_id=agreement.id, task_id=task_id, cmds=cmds)
                                emit(evt)
                                if isinstance(evt, events.CommandExecuted) and not evt.success:
                                    raise CommandExecutionError(evt.command, evt.message)

                            emit(events.GettingResults(agr_id=agreement.id, task_id=task_id))
                            await batch.post()
                            emit(events.ScriptFinished(agr_id=agreement.id, task_id=task_id))
                            await accept_payment_for_agreement(agreement.id, partial=True)

                        except Exception:

                            try:
                                await command_generator.athrow(*sys.exc_info())
                            except Exception:
                                if self._conf.traceback:
                                    traceback.print_exc()
                                emit(
                                    events.WorkerFinished(
                                        agr_id=agreement.id, exc_info=sys.exc_info()  # type: ignore
                                    )
                                )
                                raise

            await accept_payment_for_agreement(agreement.id)
            emit(events.WorkerFinished(agr_id=agreement.id))

        async def worker_starter() -> None:
            while True:
                await asyncio.sleep(2)
                await agreements_pool.cycle()
                if len(workers) < self._conf.max_workers and work_queue.has_unassigned_items():
                    new_task = None
                    try:
                        new_task = await agreements_pool.use_agreement(
                            lambda agreement, node: loop.create_task(start_worker(agreement, node))
                        )
                        if new_task is None:
                            continue
                        workers.add(new_task)
                    except CancelledError:
                        raise
                    except Exception:
                        if self._conf.traceback:
                            traceback.print_exc()
                        if new_task:
                            new_task.cancel()
                        logger.debug("There was a problem during use_agreement", exc_info=True)

        loop = asyncio.get_event_loop()
        find_offers_task = loop.create_task(find_offers())
        process_invoices_job = loop.create_task(process_invoices())
        wait_until_done = loop.create_task(work_queue.wait_until_done())
        worker_starter_task = loop.create_task(worker_starter())
        debit_notes_job = loop.create_task(process_debit_notes())
        # Py38: find_offers_task.set_name('find_offers_task')

        get_offers_deadline = datetime.now(timezone.utc) + self._conf.get_offers_timeout
        get_done_task: Optional[asyncio.Task] = None
        services.update(
            {
                find_offers_task,
                process_invoices_job,
                wait_until_done,
                worker_starter_task,
                debit_notes_job,
            }
        )
        cancelled = False

        try:
            while wait_until_done in services or not done_queue.empty():

                now = datetime.now(timezone.utc)
                if now > self._expires:
                    raise TimeoutError(f"Computation timed out after {self._conf.timeout}")
                if now > get_offers_deadline and proposals_confirmed == 0:
                    emit(
                        events.NoProposalsConfirmed(
                            num_offers=offers_collected, timeout=self._conf.get_offers_timeout
                        )
                    )
                    get_offers_deadline += self._conf.get_offers_timeout

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
                            if self._conf.traceback:
                                traceback.print_exc()

                workers -= done
                services -= done

                assert get_done_task
                if get_done_task.done():
                    yield get_done_task.result()
                    assert get_done_task not in services
                    get_done_task = None

            emit(events.ComputationFinished())

        except (Exception, CancelledError, KeyboardInterrupt):
            emit(events.ComputationFinished(exc_info=sys.exc_info()))  # type: ignore
            cancelled = True

        finally:
            # Importing this at the beginning would cause circular dependencies
            from ..log import pluralize

            payment_closing = True
            for task in services:
                if task is not process_invoices_job:
                    task.cancel()
            if agreements_pool.confirmed == 0:
                # No need to wait for invoices
                process_invoices_job.cancel()
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
                await agreements_pool.terminate_all(reason=reason)
            except Exception:
                logger.debug("Problem with agreements termination", exc_info=True)
                if self._conf.traceback:
                    traceback.print_exc()

            try:
                logger.info("Waiting for all services to finish...")
                _, pending = await asyncio.wait(
                    workers.union(services), timeout=10, return_when=asyncio.ALL_COMPLETED
                )
                if pending:
                    logger.debug(
                        "%s still running: %s", pluralize(len(pending), "service"), pending
                    )
            except Exception:
                if self._conf.traceback:
                    traceback.print_exc()

            if agreements_to_pay:
                logger.info(
                    "%s still unpaid, waiting for invoices...",
                    pluralize(len(agreements_to_pay), "agreement"),
                )
                await asyncio.wait(
                    {process_invoices_job}, timeout=30, return_when=asyncio.ALL_COMPLETED
                )
                if agreements_to_pay:
                    logger.warning("Unpaid agreements: %s", agreements_to_pay)

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
                            expires=self._expires + CFG_INVOICE_TIMEOUT,
                        )
                    ),
                )
                self._budget_allocations.append(allocation)

            if not self._budget_allocations:
                raise NoPaymentAccountError(self._driver, self._network)

        allocation_ids = [allocation.id for allocation in self._budget_allocations]
        return await self._payment_api.decorate_demand(allocation_ids)

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
            for allocation in self._budget_allocations
            if allocation.payment_platform is not None
        }
        return req_platforms.intersection(prov_platforms)

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

    async def __aenter__(self) -> "Executor":
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
        logger.debug("Executor is shutting down...")
        emit = cast(Callable[[Event], None], self._wrapped_consumer.async_call)
        # Wait until all computations are finished
        await asyncio.gather(*[event.wait() for event in self._active_computations])
        # TODO: prevent new computations at this point (if it's even possible to start one)
        try:
            await self._stack.aclose()
            emit(events.ShutdownFinished())
        except Exception:
            emit(events.ShutdownFinished(exc_info=sys.exc_info()))
        finally:
            await self._wrapped_consumer.stop()
