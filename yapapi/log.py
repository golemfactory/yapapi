"""Utilities for logging computation events via the standard `logging` module.

Functions in this module fall into two categories:

* Functions that convert computation events generated by the `Executor.submit`
method to calls to the standard Python `logging` module, using the loggers
in the "yapapi" namespace (e.g. `logging.getLogger("yapapi.executor")`).
These functions should be passed as `event_consumer` arguments to `Executor()`.

* Functions that perform configuration of the `logging` module itself.
Since logging configuration is in general a responsibility of the code that
uses `yapapi` as a library, we only provide the `enable_default_logger`
function in this category, that enables logging to stderr with level `logging.INFO`
and, optionally, to a given file with level `logging.DEBUG`.


Functions for handling events
-----------------------------

Several functions from this module can be passed as `event_consumer` callback to
`yapapi.Executor()`.

For detailed, human-readable output use the `log_event` function:
```python
    Executor(..., event_consumer=yapapi.log.log_event)
```
For even more detailed, machine-readable output use `log_event_repr`:
```python
    Executor(..., event_consumer=yapapi.log.log_event_repr)
```
For summarized, human-readable output use `log_summary()`:
```python
    Executor(..., event_consumer=yapapi.log.log_summary())
```
Summary output can be combined with a detailed one by passing the detailed logger
as an argument to `log_summary`:
```python
    Executor(
        ...
        event_consumer=yapapi.log.log_summary(yapapi.log.log_event_repr)
    )
```
"""
from asyncio import get_event_loop, CancelledError
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import itertools
import inspect
import logging
import os
import sys
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

if sys.version_info >= (3, 8):
    from typing import Final
else:
    from typing_extensions import Final

from yapapi import events, __version__ as yapapi_version
from yapapi.services import MAX_AGREEMENT_EXPIRATION, MIN_AGREEMENT_EXPIRATION
from yapapi.rest.activity import CommandExecutionError
from yapapi.utils import get_local_timezone, get_logger


event_logger = logging.getLogger("yapapi.events")

# Initializing loggers, so that logger.setLevel() in enable_default_logger will work.
_agreements_pool_logger = logging.getLogger("yapapi.agreements_pool")

REPORT_CONFIRMED_PROVIDERS_INTERVAL: Final[float] = 3.0


class _YagnaDatetimeFormatter(logging.Formatter):
    """Custom log Formatter that formats datetime using the same convention yagna uses."""

    LOCAL_TZ = get_local_timezone()

    def formatTime(self, record: logging.LogRecord, datefmt=None):
        """Format datetime; example: `2021-06-11T14:55:43.156.123+0200`."""
        dt = datetime.fromtimestamp(record.created, tz=self.LOCAL_TZ)
        millis = f"{(dt.microsecond // 1000):03d}"
        return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{millis}%z")


def enable_default_logger(
    format_: str = "[%(asctime)s %(levelname)s %(name)s] %(message)s",
    log_file: Optional[str] = None,
    debug_activity_api: bool = False,
    debug_market_api: bool = False,
    debug_payment_api: bool = False,
    debug_net_api: bool = False,
):
    """Enable the default logger that logs messages to stderr with level `INFO`.

    If `log_file` is specified, the logger with output messages with level `DEBUG` to
    the given file.
    """
    logger = logging.getLogger("yapapi")
    logger.setLevel(logging.DEBUG)
    logger.disabled = False

    formatter = _YagnaDatetimeFormatter(fmt=format_)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(filename=log_file, mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        logger.debug(
            "Yapapi version: %s, script: %s, working directory: %s",
            yapapi_version,
            sys.argv[0],
            os.getcwd(),
        )
        logger.info(
            "Using log file `%s`; in case of errors look for additional information there", log_file
        )

        for flag, logger_name in (
            (debug_activity_api, "ya_activity"),
            (debug_market_api, "ya_market"),
            (debug_payment_api, "ya_payment"),
            (debug_net_api, "ya_net"),
        ):
            if flag:
                api_logger = logging.getLogger(logger_name)
                api_logger.setLevel(logging.DEBUG)
                api_logger.addHandler(file_handler)


# Default human-readable representation of event types.
event_type_to_string = {
    events.ComputationStarted: "Computation started",
    events.ComputationFinished: "Computation finished",
    events.SubscriptionCreated: "Demand published on the market",
    events.SubscriptionFailed: "Demand publication failed",
    events.CollectFailed: "Failed to collect proposals for demand",
    events.NoProposalsConfirmed: "No proposals confirmed by providers",
    events.ProposalReceived: "Proposal received from the market",
    events.ProposalRejected: "Proposal rejected",
    events.ProposalResponded: "Responded to a proposal",
    events.ProposalFailed: "Failed to respond to proposal",
    events.ProposalConfirmed: "Proposal confirmed by provider",
    events.AgreementCreated: "Agreement proposal sent to provider",
    events.AgreementConfirmed: "Agreement approved by provider",
    events.AgreementRejected: "Agreement rejected by provider",
    events.AgreementTerminated: "Agreement terminated",
    events.DebitNoteAccepted: "Debit note accepted",
    events.DebitNoteReceived: "Debit note received",
    events.PaymentFailed: "Payment failed",
    events.PaymentPrepared: "Payment prepared",
    events.PaymentQueued: "Payment queued",
    events.InvoiceAccepted: "Invoice accepted",
    events.InvoiceReceived: "Invoice received",
    events.WorkerStarted: "Worker started for agreement",
    events.ActivityCreated: "Activity created on provider",
    events.ActivityCreateFailed: "Failed to create activity",
    events.TaskStarted: "Task started",
    events.TaskFinished: "Task finished",
    events.ServiceStarted: "Work on a service started",
    events.ServiceFinished: "Work on a service started",
    events.ScriptSent: "Script sent to provider",
    events.CommandStarted: "Command started",
    events.CommandStdOut: "Command stdout",
    events.CommandStdErr: "Command stderr",
    events.CommandExecuted: "Script command executed",
    events.GettingResults: "Getting script results",
    events.ScriptFinished: "Script finished",
    events.TaskAccepted: "Task accepted",
    events.TaskRejected: "Task rejected",
    events.WorkerFinished: "Worker finished",
    events.DownloadStarted: "Download started",
    events.DownloadFinished: "Download finished",
    events.ShutdownFinished: "Shutdown finished",
    events.ExecutionInterrupted: "Execution interrupted",
}


def _check_event_type_to_string():
    # This is to check that `event_type_to_string` covers all event types

    # get all event types, including base classes
    event_types = set(
        member
        for member in events.__dict__.values()
        if inspect.isclass(member) and issubclass(member, events.Event)
    )

    # get only the leaf event classes (the ones which have no subclasses)
    concrete_event_types = {ev for ev in event_types if not ev.__subclasses__()}

    assert len(concrete_event_types) > 0  # Sanity check

    assert concrete_event_types.issubset(
        event_type_to_string.keys()
    ), concrete_event_types.difference(event_type_to_string.keys())


_check_event_type_to_string()


def log_event(event: events.Event) -> None:
    """Log `event` with a human-readable description."""

    loglevel = logging.DEBUG

    if not event_logger.isEnabledFor(loglevel):
        return

    descr = event_type_to_string[type(event)]
    msg = "; ".join([descr, *(f"{name} = {value}" for name, value in event.__dict__.items())])

    if event.exc_info:
        event_logger.log(loglevel, msg, exc_info=event.exc_info)
    else:
        event_logger.log(loglevel, msg)


def log_event_repr(event: events.Event) -> None:
    """Log the result of calling `repr(event)`."""

    if event.exc_info:
        event_logger.debug("%r", event, exc_info=event.exc_info)
    else:
        event_logger.debug("%r", event)


@dataclass(frozen=True)
class ProviderInfo:
    id: str
    name: str
    subnet_tag: Optional[str]


MAX_AGREEMENT_EXPIRATION_MINUTES = round(MAX_AGREEMENT_EXPIRATION.seconds / 60)
MIN_AGREEMENT_EXPIRATION_MINUTES = round(MIN_AGREEMENT_EXPIRATION.seconds / 60)


# Some type aliases to make types more meaningful
AgreementId = str
JobId = str
TaskId = str
ProposalId = str
ProviderId = str


class SummaryLogger:
    """Aggregates information from computation events to provide a high-level summary.

    The logger's :func:`log()` method can be used as `event_consumer` callback
    in :class:`~yapapi.Golem` initialization. It will aggregate the events generated
    and output some summary information.

    The optional `wrapped_emitter` argument can be used for chaining event
    emitters: each event logged with :func:`log()` is first passed to
    `wrapped_emitter`.

    For example, with the following setup, each event
    will be logged by `log_event_repr`, and additionally, certain events
    will cause summary messages to be logged.
    ::

        detailed_logger = log_event_repr
        summary_logger = SummaryLogger(wrapped_emitter=detailed_logger).log
        golem = Golem(..., event_consumer=summary_logger)
    """

    # Define some messages a string constants here, so they can be used e.g. in
    # integration tests, in assertions that expect certain messages to be logged
    GOLEM_SHUTDOWN_SUCCESSFUL_MESSAGE = "Golem engine has shut down"

    logger = get_logger("yapapi.summary")

    # Generates subsequent numbers, for use in generated provider names
    numbers: Iterator[int]

    # Start time of the computation, indexed by job id
    start_time: Dict[JobId, float]

    # Maps received proposal ids to provider ids
    received_proposals: Dict[ProposalId, ProviderId]

    # Set of confirmed proposal ids
    confirmed_proposals: Set[ProposalId]

    # Last number of confirmed providers
    prev_confirmed_providers: int

    # Maps agreement ids to provider infos
    agreement_provider_info: Dict[AgreementId, ProviderInfo]

    # Set of agreements confirmed by providers, indexed by job id
    confirmed_agreements: Dict[JobId, Set[AgreementId]]

    # Maps task id to task data
    task_data: Dict[TaskId, Any]

    # Maps a job id and provider info to the list of task ids computed
    # by the provider for the given job
    provider_tasks: Dict[JobId, Dict[ProviderInfo, List[TaskId]]]

    # Map a provider info to the sum of amounts in this provider's invoices
    provider_cost: Dict[ProviderInfo, Decimal]

    # Count how many times a worker failed on a provider
    provider_failures: Dict[JobId, Dict[ProviderInfo, int]]

    # Has computation been cancelled?
    cancelled: bool

    # Has computation finished?
    finished: bool

    # Has Executor shut down?
    shutdown_complete: bool = False

    # Total time waiting for the first proposal
    time_waiting_for_proposals: timedelta

    def __init__(self, wrapped_emitter: Optional[Callable[[events.Event], None]] = None):
        """Create a SummaryLogger."""

        self._wrapped_emitter = wrapped_emitter
        self.numbers: Iterator[int] = itertools.count(1)
        self._reset_counters()
        self._print_confirmed_providers()

    def _reset_counters(self):
        """Reset all information aggregated by this logger related to a single Executor instance."""

        self.provider_cost = {}
        self.start_time = {}
        self.received_proposals = {}
        self.confirmed_proposals = set()
        self.agreement_provider_info = {}
        self.confirmed_agreements = defaultdict(set)
        self.task_data = {}
        self.script_cmds = {}
        self.provider_cost = {}
        self.provider_tasks = defaultdict(lambda: defaultdict(list))
        self.provider_failures = defaultdict(Counter)
        self.cancelled = False
        self.finished = False
        self.error_occurred = False
        self.time_waiting_for_proposals = timedelta(0)
        self.prev_confirmed_providers = 0

    def _register_job(self, job_id: str) -> None:
        """Initialize counters for a new job."""
        self.start_time[job_id] = time.time()

    def _print_summary(self, job_id: str) -> None:
        """Print a summary at the end of computation."""

        num_providers = len(
            {self.agreement_provider_info[agr_id] for agr_id in self.confirmed_agreements[job_id]}
        )
        self.logger.info(
            "Negotiated %d agreements with %s",
            len(self.confirmed_agreements[job_id]),
            pluralize(num_providers, "provider"),
            job_id=job_id,
        )
        for info, tasks in self.provider_tasks[job_id].items():
            self.logger.info(
                "Provider '%s' computed %s",
                info.name,
                pluralize(len(tasks), "task"),
                job_id=job_id,
            )
        for agr_id in self.confirmed_agreements[job_id]:
            info = self.agreement_provider_info[agr_id]
            if info not in self.provider_tasks[job_id]:
                self.logger.info(
                    "Provider '%s' did not compute any tasks", info.name, job_id=job_id
                )
        for info, num in self.provider_failures[job_id].items():
            self.logger.info(
                "Activity failed %s on provider '%s'",
                pluralize(num, "time"),
                info.name,
                job_id=job_id,
            )

    def _print_total_cost(self, partial: bool = False) -> None:
        """Print the sum of all accepted invoices."""

        total_cost = sum(self.provider_cost.values(), Decimal(0))
        label = "Total cost" if not partial else "The cost so far"
        self.logger.info("%s: %s", label, total_cost.normalize())

    def _print_confirmed_providers(self) -> None:
        confirmed_providers = set(
            self.received_proposals[prop_id] for prop_id in self.confirmed_proposals
        )
        if self.prev_confirmed_providers < len(confirmed_providers):
            self.logger.info(
                "Received proposals from %s so far",
                pluralize(len(confirmed_providers), "provider"),
            )
            self.prev_confirmed_providers = len(confirmed_providers)
        if not self.shutdown_complete:
            get_event_loop().call_later(
                REPORT_CONFIRMED_PROVIDERS_INTERVAL,
                self._print_confirmed_providers,
            )

    def log(self, event: events.Event) -> None:
        """Register an event."""

        if self._wrapped_emitter:
            self._wrapped_emitter(event)

        if self.error_occurred:
            return

        try:
            self._handle(event)
        except Exception:
            self.logger.exception("SummaryLogger entered invalid state")
            self.error_occurred = True

    def _handle(self, event: events.Event):
        if isinstance(event, events.ComputationStarted):
            self._register_job(event.job_id)
            if self.provider_cost:
                # This means another computation run in the current Executor instance.
                self._print_total_cost(partial=True)
            timeout = event.expires - datetime.now(timezone.utc)
            # Compute the timeout as it will be seen by providers, assuming they will see
            # the Demand 5 seconds from now
            provider_timeout = timeout - timedelta(seconds=5)
            if not MIN_AGREEMENT_EXPIRATION <= provider_timeout <= MAX_AGREEMENT_EXPIRATION:
                min, sec = divmod(round(timeout.total_seconds()), 60)
                seconds_str = f" {sec} sec " if sec else " "
                self.logger.warning(
                    f"Expiration time for your tasks is set to {min} min{seconds_str}from now."
                    f" Providers may not be willing to take up tasks which expire sooner than"
                    f" {MIN_AGREEMENT_EXPIRATION_MINUTES} min or later than"
                    f" {MAX_AGREEMENT_EXPIRATION_MINUTES} min, counting"
                    f" from the moment they get your demand."
                )

        elif isinstance(event, events.ProposalReceived):
            self.received_proposals[event.prop_id] = event.provider_id

        elif isinstance(event, events.ProposalConfirmed):
            self.confirmed_proposals.add(event.prop_id)

        elif isinstance(event, events.NoProposalsConfirmed):
            self.time_waiting_for_proposals += event.timeout
            if event.num_offers == 0:
                msg = (
                    "No offers have been collected from the market for"
                    f" {self.time_waiting_for_proposals.seconds}s."
                )
            else:
                msg = (
                    f"{event.num_offers} {'offer has' if event.num_offers == 1 else 'offers have'} "
                    f"been collected from the market, but no provider has responded for "
                    f"{self.time_waiting_for_proposals.seconds}s."
                )
            msg += (
                f" Make sure you're using the latest released versions of yagna and yapapi,"
                f" and the correct subnet. Also make sure that the timeout for computing all"
                f" tasks is within the {MIN_AGREEMENT_EXPIRATION_MINUTES} min to"
                f" {MAX_AGREEMENT_EXPIRATION_MINUTES} min range."
            )
            self.logger.warning(msg)

        elif isinstance(event, events.AgreementCreated):
            provider_name = event.provider_info.name or event.provider_id
            self.logger.info(
                "Agreement proposed to provider '%s'", provider_name, job_id=event.job_id
            )
            self.agreement_provider_info[event.agr_id] = ProviderInfo(
                event.provider_id, provider_name, event.provider_info.subnet_tag
            )

        elif isinstance(event, events.AgreementConfirmed):

            self.logger.info(
                "Agreement confirmed by provider '%s'",
                self.agreement_provider_info[event.agr_id].name,
                job_id=event.job_id,
            )
            self.confirmed_agreements[event.job_id].add(event.agr_id)

        elif isinstance(event, events.TaskStarted):
            provider_info = self.agreement_provider_info[event.agr_id]
            self.task_data[event.task_id] = event.task_data
            self.logger.info(
                "Task started on provider '%s', task data: %s",
                provider_info.name,
                str_capped(event.task_data, 200),
                job_id=event.job_id,
            )

        elif isinstance(event, events.TaskFinished):
            provider_info = self.agreement_provider_info[event.agr_id]
            data = self.task_data[event.task_id]
            self.logger.info(
                "Task finished by provider '%s', task data: %s",
                provider_info.name,
                str_capped(data, 200),
                job_id=event.job_id,
            )
            if event.task_id:
                self.provider_tasks[event.job_id][provider_info].append(event.task_id)

        elif isinstance(event, events.ScriptSent):
            provider_info = self.agreement_provider_info[event.agr_id]
            self.script_cmds[event.script_id] = event.cmds
            cmds = ", ".join([", ".join(cmd.keys()) for cmd in event.cmds])
            self.logger.debug(
                "Script '%s' sent to provider '%s', cmds: %s",
                event.script_id,
                provider_info.name,
                str_capped(cmds, 200),
                job_id=event.job_id,
            )

        elif isinstance(event, events.ScriptFinished):
            provider_info = self.agreement_provider_info[event.agr_id]
            self.logger.debug(
                "Script '%s' finished on provider '%s'",
                event.script_id,
                provider_info.name,
                job_id=event.job_id,
            )

        elif isinstance(event, events.InvoiceAccepted):
            provider_info = self.agreement_provider_info[event.agr_id]
            cost = self.provider_cost.get(provider_info, Decimal(0))
            cost += Decimal(event.amount)
            self.provider_cost[provider_info] = cost
            self.logger.info(
                "Accepted invoice from '%s', amount: %s",
                provider_info.name,
                cost.normalize(),
                job_id=event.job_id,
            )

        elif isinstance(event, events.PaymentFailed):
            assert event.exc_info
            _exc_type, exc, _tb = event.exc_info
            provider_info = self.agreement_provider_info[event.agr_id]
            reason = str(exc) or repr(exc) or "unexpected error"
            self.logger.error(
                "Failed to accept an invoice or a debit note from '%s', reason: %s",
                provider_info,
                reason,
                job_id=event.job_id,
            )

        elif isinstance(event, events.WorkerFinished):
            if event.exc_info is None or self.cancelled:
                return
            _exc_type, exc, _tb = event.exc_info
            provider_info = self.agreement_provider_info[event.agr_id]
            self.provider_failures[event.job_id][provider_info] += 1
            reason = str(exc) or repr(exc) or "unexpected error"
            if isinstance(exc, CommandExecutionError):
                self.logger.warning(
                    "Activity failed on provider '%s'; reason: %s",
                    provider_info.name,
                    reason,
                    job_id=event.job_id,
                )
            else:
                self.logger.warning(
                    "Worker for provider '%s' failed; reason: %s",
                    provider_info.name,
                    reason,
                    job_id=event.job_id,
                )

        elif isinstance(event, events.ComputationFinished):
            job_id = event.job_id
            if not event.exc_info:
                total_time = time.time() - self.start_time[job_id]
                self.logger.info(f"Computation finished in {total_time:.1f}s", job_id=job_id)
                self.finished = True
            else:
                _exc_type, exc, _tb = event.exc_info
                if isinstance(exc, CancelledError):
                    self.cancelled = True
                    self.logger.warning("Computation cancelled", job_id=job_id)
                else:
                    reason = str(exc) or repr(exc) or "unexpected error"
                    self.logger.error("Computation failed, reason: %s", reason, job_id=job_id)
            self._print_summary(job_id)

        elif isinstance(event, events.ShutdownFinished):
            self._print_total_cost()
            self.provider_cost = {}
            if not event.exc_info:
                self.logger.info(SummaryLogger.GOLEM_SHUTDOWN_SUCCESSFUL_MESSAGE)
            else:
                _exc_type, exc, _tb = event.exc_info
                reason = str(exc) or repr(exc) or "unexpected error"
                self.logger.error("Error when shutting down Golem engine: %s", reason)
            self.shutdown_complete = True

        elif isinstance(event, events.AgreementTerminated):
            if event.reason.get("golem.requestor.code") == "Success":
                pass
            else:
                prov_info = self.agreement_provider_info[event.agr_id]
                self.logger.info(f"Terminated agreement with {prov_info.name}", job_id=event.job_id)

        elif isinstance(event, events.ExecutionInterrupted):
            assert event.exc_info
            exc_type = event.exc_info[0]
            self.logger.warning(f"Execution interrupted by {exc_type.__name__}")


def log_summary(wrapped_emitter: Optional[Callable[[events.Event], None]] = None):
    """Output a summary of computation.

    This is a utility function that creates a :class:`SummaryLogger` instance
    wrapping an optional `wrapped_emitter` and returns its :func:`~SummaryLogger.log` method.

    See the documentation of :class:`SummaryLogger` for more information.
    """
    summary_logger = SummaryLogger(wrapped_emitter)
    return summary_logger.log


def pluralize(num: int, thing: str) -> str:
    """Return the string f"1 {thing}" or f"{num} {thing}s", depending on `num`."""
    return f"1 {thing}" if num == 1 else f"{num} {thing}s"


def str_capped(object: Any, max_len: int) -> str:
    """Return the string representation of `object` trimmed to `max_len`.

    Trailing ellipsis is added to the returned string if the original had to be trimmed.
    """
    s = str(object)
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..." if max_len >= 3 else "..."
