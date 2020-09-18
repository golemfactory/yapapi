"""Utilities for yapapi example scripts."""
import argparse
from collections import defaultdict
from itertools import count
import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

from yapapi.runner.events import Event, EventType, event_type_to_string


def build_parser(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--subnet-tag", default="testnet")
    parser.add_argument(
        "--debug", dest="log_level", action="store_const", const=logging.DEBUG, default=logging.INFO
    )
    return parser


class SummaryLogger:
    """Aggregates information from computation events to provide a high-level description."""

    logger = logging.getLogger("yapapi.summary")

    # Generates subsequent numbers, for use in generated provider names
    numbers: Iterator[int]

    # Start time of the computation
    start_time: float

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

    # Has computation finished?
    finished: bool

    def __init__(self, wrapped_emitter: Optional[Callable[[Event], None]] = None):
        """Create a SummaryLogger.

        The logger's `log()` method can be used as `event_emitter` callback
        to `Engine.map()`.

        The `wrapped_emitter` argument can be used for chaining event emitters:
        each event logged with `log()` is first passed to `wrapped_emitter`.

        Example use:
        ```
            detailed_logger = yapapi.log.log_event_json
            summary_logger = SummaryLogger(wrapped_emitter=detailed_logger)
            engine = yapapi.runner.Engine(..., event_emitter=summary_logger)
        ```

        With this setup, each event emitted by `engine` will be logged by
        `log_event_json`, and additionally, certain events will cause summary
        messages to be logged by `summary_logger`.
        """
        self._wrapped_emitter = wrapped_emitter
        self.numbers: Iterator[int] = count(1)
        self._reset()

    def _reset(self) -> None:
        """Reset all information aggregated by this logger."""

        self.start_time = time.time()
        self.confirmed_proposals = set()
        self.agreement_provider_name = {}
        self.confirmed_agreements = set()
        self.task_data = {}
        self.provider_tasks = defaultdict(list)
        self.provider_cost = {}
        self.finished = False
        self.error_occurred = False

    def _print_total_cost(self) -> None:

        if not self.finished:
            return

        provider_names = set(self.provider_tasks.keys())
        if set(self.provider_cost).issuperset(provider_names):
            total_cost = sum(self.provider_cost.values(), 0.0)
            self.logger.info("Total cost: %s", total_cost)

    def log(self, event: EventType) -> None:
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

    def _handle(self, event: EventType):

        if isinstance(event, Event.ComputationStarted):
            self._reset()

        if isinstance(event, Event.SubscriptionCreated):
            self.logger.info(event_type_to_string[type(event)])

        elif isinstance(event, Event.ProposalConfirmed):
            self.confirmed_proposals.add(event.prop_id)
            self.logger.info(
                "Received proposals from %s providers so far", len(self.confirmed_proposals)
            )

        elif isinstance(event, Event.AgreementCreated):
            provider_name = event.provider_id.name
            if not provider_name:
                provider_name = f"provider-{next(self.numbers)}"
            self.logger.info("Agreement proposed to provider '%s'", provider_name)
            self.agreement_provider_name[event.agr_id] = provider_name

        elif isinstance(event, Event.AgreementConfirmed):
            self.logger.info(
                "Agreement confirmed by provider '%s'", self.agreement_provider_name[event.agr_id]
            )
            self.confirmed_agreements.add(event.agr_id)

        elif isinstance(event, Event.TaskStarted):
            self.task_data[event.task_id] = event.task_data

        elif isinstance(event, Event.ScriptSent):
            provider_name = self.agreement_provider_name[event.agr_id]
            self.logger.info(
                "Task sent to provider '%s', task data: %s",
                provider_name,
                self.task_data[event.task_id],
            )

        elif isinstance(event, Event.ScriptFinished):
            provider_name = self.agreement_provider_name[event.agr_id]
            self.logger.info(
                "Task computed by provider '%s', task data: %s",
                provider_name,
                self.task_data[event.task_id],
            )
            self.provider_tasks[provider_name].append(event.task_id)

        elif isinstance(event, Event.InvoiceReceived):
            provider_name = self.agreement_provider_name[event.agr_id]
            cost = self.provider_cost.get(provider_name, 0.0)
            cost += float(event.amount)
            self.provider_cost[provider_name] = cost
            self._print_total_cost()

        elif isinstance(event, Event.ComputationFinished):
            self.finished = True
            total_time = time.time() - self.start_time
            self.logger.info(f"Computation finished in {total_time:.1f}s")
            self.logger.info(
                "Negotiated agreements with %s providers", len(self.confirmed_agreements)
            )
            for provider_name, tasks in self.provider_tasks.items():
                self.logger.info("Provider '%s' computed %s tasks", provider_name, len(tasks))
            self._print_total_cost()
