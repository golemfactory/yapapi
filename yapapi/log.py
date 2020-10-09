"""Utilities for logging computation events via the standard `logging` module.

Functions in this module fall into two categories:

* Functions that convert computation events generated by the `Engine.map`
method to calls to the standard Python `logging` module, using the loggers
in the "yapapi" namespace (e.g. `logging.getLogger("yapapi.runner")`).
These functions should be passed as `event_emitter` arguments to `Engine()`.

* Functions that perform configuration of the `logging` module itself.
Since logging configuration is in general a responsibility of the code that
uses `yapapi` as a library, we only provide the `enable_default_logger`
function in this category, that enables logging to stderr with level `logging.INFO`
and, optionally, to a given file with level `logging.DEBUG`.


Functions for handling events
-----------------------------

Several function from this module can be passed as `event_emitter` callback to
`yapapi.runner.Engine()`.

For detailed human-readable output use the `log_event` function:
```
    Engine(..., event_emitter=yapapi.log.log_event)
```
For even more detailed machine-readable output use `log_event_repr`:
```
    Engine(..., event_emitter=yapapi.log.log_event_repr)
```
For summary human-readable output use `log_summary()`:
```
    Engine(..., event_emitter=yapapi.log.log_summary())
```
Summary output can be combined with a detailed one by passing a detailed logger
as an argument to `log_summary`:
```
    Engine(
        ...
        event_emitter=yapapi.log.log_summary(yapapi.log.log_event_repr)
    )
```
"""
from collections import defaultdict, Counter
from dataclasses import asdict
from datetime import timedelta
import itertools
import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

import yapapi.runner.events as events


logger = logging.getLogger("yapapi.runner")


def enable_default_logger(
    format_: str = "[%(asctime)s %(levelname)s %(name)s] %(message)s",
    log_file: Optional[str] = None,
):
    """Enable the default logger that logs messages to stderr with level `INFO`.

    If `log_file` is specified, the logger with output messages with level `DEBUG` to
    the given file.
    """
    logger = logging.getLogger("yapapi")
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    formatter = logging.Formatter(format_)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(filename=log_file, mode="w")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)


# Default human-readable representation of event types.
event_type_to_string = {
    events.ComputationStarted: "Computation started",
    events.ComputationFinished: "Computation finished",
    events.ComputationFailed: "Computation failed",
    events.SubscriptionCreated: "Demand published on the market",
    events.SubscriptionFailed: "Demand publication failed",
    events.CollectFailed: "Failed to collect proposals for demand",
    events.NoProposalsConfirmed: "No proposals confirmed by providers",
    events.ProposalReceived: "Proposal received from the market",
    events.ProposalRejected: "Proposal rejected",  # by who? alt: Rejected a proposal?
    events.ProposalResponded: "Responded to a proposal",
    events.ProposalFailed: "Failed to respond to proposal",
    events.ProposalConfirmed: "Proposal confirmed by provider",  # ... negotiated with provider?
    events.AgreementCreated: "Agreement proposal sent to provider",
    events.AgreementConfirmed: "Agreement approved by provider",
    events.AgreementRejected: "Agreement rejected by provider",
    events.PaymentAccepted: "Payment accepted",  # by who?
    events.PaymentPrepared: "Payment prepared",
    events.PaymentQueued: "Payment queued",
    events.InvoiceReceived: "Invoice received",  # by who?
    events.WorkerStarted: "Worker started for agreement",
    events.ActivityCreated: "Activity created on provider",
    events.ActivityCreateFailed: "Failed to create activity",
    events.TaskStarted: "Task started",
    events.ScriptSent: "Script sent to provider",
    events.CommandStarted: "Command started",
    events.CommandStdOut: "Command stdout",
    events.CommandStdErr: "Command stderr",
    events.CommandExecuted: "Script command executed",
    events.GettingResults: "Getting script results",
    events.ScriptFinished: "Script finished",
    events.TaskAccepted: "Task accepted",  # by who?
    events.TaskRejected: "Task rejected",  # by who?
    events.WorkerFinished: "Worker finished",
    events.DownloadStarted: "Download started",
    events.DownloadFinished: "Download finished",
}


def _check_event_type_to_string():
    # This is to check that `event_type_to_string` covers all event types

    _event_types = set(
        member
        for member in events.__dict__.values()
        if type(member) is type and issubclass(member, events.Event)
    )

    _event_types_superclasses = {ty for ev in _event_types for ty in ev.mro() if ty is not ev}

    _concrete_event_types = _event_types.difference(_event_types_superclasses)

    assert len(_concrete_event_types) > 0  # Sanity check

    assert _concrete_event_types.issubset(
        event_type_to_string.keys()
    ), _concrete_event_types.difference(event_type_to_string.keys())


_check_event_type_to_string()


def log_event(event: events.Event) -> None:
    """Log an event in human-readable format."""

    loglevel = logging.DEBUG

    def _format(obj: Any, max_len: int = 200) -> str:
        # This will also escape control characters, in particular,
        # newline characters in `obj` will be replaced by r"\n".
        text = repr(obj)
        if len(text) > max_len:
            text = text[: max_len - 3] + "..."
        return text

    if not logger.isEnabledFor(loglevel):
        return

    msg = event_type_to_string[type(event)]
    info = "; ".join(f"{name} = {_format(value)}" for name, value in asdict(event).items())
    if info:
        msg += "; " + info
    logger.log(loglevel, msg)


def log_event_repr(event: events.Event) -> None:
    """Log the result of calling `__repr__()` for the `event`."""
    exc_info, _ = event.extract_exc_info()
    logger.debug("%r", event, exc_info=exc_info)


class SummaryLogger:
    """Aggregates information from computation events to provide a high-level description.

    The logger's `log()` method can be used as `event_emitter` callback
    to `Engine()`. It will aggregate the events generated by `Engine.map()`
    and output some summary information.

    The optional `wrapped_emitter` argument can be used for chaining event
    emitters: each event logged with `log()` is first passed to
    `wrapped_emitter`.

    For example, with the following setup, each event emitted by `engine`
    will be logged by `log_event_repr`, and additionally, certain events
    will cause summary messages to be logged.
    ```
        detailed_logger = log_event_repr
        summary_logger = SummaryLogger(wrapped_emitter=detailed_logger).log
        engine = Engine(..., event_emitter=summary_logger)
    ```
    """

    logger = logging.getLogger("yapapi.summary")

    # Generates subsequent numbers, for use in generated provider names
    numbers: Iterator[int]

    # Start time of the computation
    start_time: float

    # Maps received proposal ids to provider ids
    received_proposals: Dict[str, str]

    # Set of confirmed proposal ids
    confirmed_proposals: Set[str]

    # Maps agreement ids to provider names
    agreement_provider_name: Dict[str, str]

    # Set of agreements confirmed by providers
    confirmed_agreements: Set[str]

    # Maps task id to task data
    task_data: Dict[str, Any]

    # Maps a provide name to the list of task ids computed by the provider
    provider_tasks: Dict[str, List[str]]

    # Map a provider name to the sum of amounts in this provider's invoices
    provider_cost: Dict[str, float]

    # Count how many times a worker failed on a provider
    provider_failures: Dict[str, int]

    # Has computation finished?
    finished: bool

    # Total time waiting for the first proposal
    time_waiting_for_proposals: timedelta

    def __init__(self, wrapped_emitter: Optional[Callable[[events.Event], None]] = None):
        """Create a SummaryLogger."""

        self._wrapped_emitter = wrapped_emitter
        self.numbers: Iterator[int] = itertools.count(1)
        self._reset()

    def _reset(self) -> None:
        """Reset all information aggregated by this logger."""

        self.start_time = time.time()
        self.received_proposals = {}
        self.confirmed_proposals = set()
        self.agreement_provider_name = {}
        self.confirmed_agreements = set()
        self.task_data = {}
        self.provider_tasks = defaultdict(list)
        self.provider_cost = {}
        self.provider_failures = Counter()
        self.finished = False
        self.error_occurred = False
        self.time_waiting_for_proposals = timedelta(0)

    def _print_total_cost(self) -> None:

        if not self.finished:
            return

        provider_names = set(self.provider_tasks.keys())
        if set(self.provider_cost).issuperset(provider_names):
            total_cost = sum(self.provider_cost.values(), 0.0)
            self.logger.info("Total cost: %s", total_cost)

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
            self._reset()

        if isinstance(event, events.SubscriptionCreated):
            self.logger.info(event_type_to_string[type(event)])

        elif isinstance(event, events.ProposalReceived):
            self.received_proposals[event.prop_id] = event.provider_id

        elif isinstance(event, events.ProposalConfirmed):
            self.confirmed_proposals.add(event.prop_id)
            confirmed_providers = set(
                self.received_proposals[prop_id] for prop_id in self.confirmed_proposals
            )
            self.logger.info(
                "Received proposals from %s providers so far", len(confirmed_providers)
            )

        elif isinstance(event, events.NoProposalsConfirmed):
            self.time_waiting_for_proposals += event.timeout
            if event.num_offers == 0:
                msg = (
                    "No offers have been collected from the market for"
                    f" {self.time_waiting_for_proposals.seconds}s."
                )
            else:
                msg = (
                    f"{event.num_offers} offers have been collected from the market, but"
                    f" no provider has responded for {self.time_waiting_for_proposals.seconds}s."
                )
            msg += (
                " Make sure you're using the latest released versions of yagna and yapapi,"
                " and the correct subnet."
            )
            self.logger.warning(msg)

        elif isinstance(event, events.AgreementCreated):
            provider_name = event.provider_id.name
            if not provider_name:
                provider_name = f"provider-{next(self.numbers)}"
            self.logger.info("Agreement proposed to provider '%s'", provider_name)
            self.agreement_provider_name[event.agr_id] = provider_name

        elif isinstance(event, events.AgreementConfirmed):
            self.logger.info(
                "Agreement confirmed by provider '%s'", self.agreement_provider_name[event.agr_id]
            )
            self.confirmed_agreements.add(event.agr_id)

        elif isinstance(event, events.TaskStarted):
            self.task_data[event.task_id] = event.task_data

        elif isinstance(event, events.ScriptSent):
            provider_name = self.agreement_provider_name[event.agr_id]
            self.logger.info(
                "Task sent to provider '%s', task data: %s",
                provider_name,
                self.task_data[event.task_id] if event.task_id else "<initialization>",
            )

        elif isinstance(event, events.CommandExecuted):
            if event.success:
                return
            provider_name = self.agreement_provider_name[event.agr_id]
            self.logger.warning(
                "Command failed on provider '%s', command: %s, output: %s",
                provider_name,
                event.command,
                event.message,
            )

        elif isinstance(event, events.ScriptFinished):
            provider_name = self.agreement_provider_name[event.agr_id]
            self.logger.info(
                "Task computed by provider '%s', task data: %s",
                provider_name,
                self.task_data[event.task_id] if event.task_id else "<initialization>",
            )
            if event.task_id:
                self.provider_tasks[provider_name].append(event.task_id)

        elif isinstance(event, events.InvoiceReceived):
            provider_name = self.agreement_provider_name[event.agr_id]
            cost = self.provider_cost.get(provider_name, 0.0)
            cost += float(event.amount)
            self.provider_cost[provider_name] = cost
            self._print_total_cost()

        elif isinstance(event, events.WorkerFinished):
            if event.exception is None:
                return
            exc_type, exc, tb = event.exception
            provider_name = self.agreement_provider_name[event.agr_id]
            self.provider_failures[provider_name] += 1
            self.logger.warning("Activity failed on provider '%s', reason: %r", provider_name, exc)

        elif isinstance(event, events.ComputationFinished):
            self.finished = True
            total_time = time.time() - self.start_time
            self.logger.info(f"Computation finished in {total_time:.1f}s")
            agreement_providers = {
                self.agreement_provider_name[agr_id] for agr_id in self.confirmed_agreements
            }
            self.logger.info(
                "Negotiated %s agreements with %s providers",
                len(self.confirmed_agreements),
                len(agreement_providers),
            )
            for provider_name, tasks in self.provider_tasks.items():
                self.logger.info("Provider '%s' computed %s tasks", provider_name, len(tasks))
            for provider_name in set(self.agreement_provider_name.values()):
                if provider_name not in self.provider_tasks:
                    self.logger.info("Provider '%s' did not compute any tasks", provider_name)
            for provider_name, count in self.provider_failures.items():
                self.logger.info(
                    "Activity failed %s time(s) on provider '%s'", count, provider_name
                )
            self._print_total_cost()

        elif isinstance(event, events.ComputationFailed):
            self.logger.error(f"Computation failed, reason: %s", event.reason)

        elif isinstance(event, events.CommandStarted):
            self.logger.info(
                f"Command started (task {event.task_id}, idx {event.cmd_idx}): {event.command}"
            )

        elif isinstance(event, events.CommandExecuted):
            self.logger.info(f"Command finished (task {event.task_id}, idx {event.cmd_idx}): {event.message}")

        elif isinstance(event, events.CommandStdOut):
            self.logger.info(
                f"Command stdout (task {event.task_id}, idx {event.cmd_idx}): {event.output.rstrip()}"
            )

        elif isinstance(event, events.CommandStdErr):
            self.logger.warning(
                f"Command stderr (task {event.task_id}, idx {event.cmd_idx}): {event.output.rstrip()}"
            )


def log_summary(wrapped_emitter: Optional[Callable[[events.Event], None]] = None):
    """Output a summary of computation.

    This is a utility function that creates a `SummaryLogger` instance
    wrapping an optional `wrapped_emitter` and returns its `log` method.

    See the documentation of `SummaryLogger` for more information.
    """
    summary_logger = SummaryLogger(wrapped_emitter)
    return summary_logger.log
