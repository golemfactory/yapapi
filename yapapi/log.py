from collections import defaultdict
from itertools import count
import json
import logging
import time
from typing import Any, Dict, Iterator, List, Set

from yapapi.runner.events import Event, EventType, event_type_to_string


logger = logging.getLogger("yapapi.runner")


def log_event(event: EventType, level=logging.DEBUG) -> None:
    """Log an event in human-readable format.

    This function is compatible with the `EventEmitter` protocol."""

    def _format(obj: Any, max_len: int = 200) -> str:
        # This will also escape control characters, in particular,
        # newline characters in `obj` will be replaced by r"\n".
        text = repr(obj)
        if len(text) > max_len:
            text = text[: max_len - 3] + "..."
        return text

    if not logger.isEnabledFor(level):
        return

    msg = event_type_to_string[type(event)]
    info = "; ".join(f"{name} = {_format(value)}" for name, value in event._asdict().items())
    if info:
        msg += "; " + info
    logger.log(level, msg)


def log_event_json(event: EventType) -> None:
    """Log an event in JSON format.

    This function is compatible with the `EventEmitter` protocol."""

    info = {name: str(value) for name, value in event._asdict().items()}
    logger.debug("%s %s", type(event).__name__, json.dumps(info) if info else "")


# TODO: consider moving this class to examples
class SummaryLogger:
    """Aggregates information from computation events to provide a high-level description.

    A separate instance of this class should be used for each call of `Engine.map()`.
    """

    def __init__(self):
        # Generates subsequent numbers, for use in generated provider names
        self.numbers: Iterator[int] = count(1)
        # Start time of the computation
        self.start_time: float = 0
        # Set of proposal issuer ids
        self.proposal_issuers: Set[str] = set()
        # Maps agreement ids to provider names
        self.sent_agreements: Dict[str, str] = {}
        # Set of agreements confirmed by providers
        self.confirmed_agreements: Set[str] = set()
        # Maps task id to task data
        self.task_data: Dict[str, Any] = {}
        # Maps a provider name to the list of tasks computed by the provider
        self.tasks_computed: Dict[str, List[str]] = defaultdict(list)
        self.error_occurred = False

    def log(self, event: EventType) -> None:

        if self.error_occurred:
            return

        try:
            self._handle(event)
        except KeyError as ke:
            logger.exception("SummaryLogger entered invalid state")
            self.error_occurred = True

    def _handle(self, event: EventType):

        if isinstance(event, Event.ComputationStarted):
            self.start_time = time.time()

        if isinstance(event, Event.SubscriptionCreated):
            logger.info(event_type_to_string[type(event)])

        elif isinstance(event, Event.ProposalReceived):
            self.proposal_issuers.add(event.provider_id)

        elif isinstance(event, Event.ProposalConfirmed):
            logger.info("Received proposals from %s providers so far", len(self.proposal_issuers))

        elif isinstance(event, Event.AgreementCreated):
            provider_name = event.provider_id.name
            if not provider_name:
                provider_name = f"provider-{next(self.numbers)}"
            logger.info("Agreement proposed to provider '%s'", provider_name)
            self.sent_agreements[event.agr_id] = provider_name

        elif isinstance(event, Event.AgreementConfirmed):
            logger.info("Agreement confirmed by provider '%s'", self.sent_agreements[event.agr_id])
            self.confirmed_agreements.add(event.agr_id)

        elif isinstance(event, Event.TaskStarted):
            self.task_data[event.task_id] = event.task_data

        elif isinstance(event, Event.ScriptSent):
            provider_name = self.sent_agreements[event.agr_id]
            logger.info(
                "Task sent to provider '%s', task data: %s",
                provider_name,
                self.task_data[event.task_id],
            )

        elif isinstance(event, Event.ScriptFinished):
            provider_name = self.sent_agreements[event.agr_id]
            logger.info(
                "Task computed by provider '%s', task data: %s",
                provider_name,
                self.task_data[event.task_id],
            )
            self.tasks_computed[provider_name].append(event.task_id)

        elif isinstance(event, Event.ComputationFinished):
            total_time = time.time() - self.start_time
            logger.info(f"Summary: Computation finished in {total_time:.1f}s")
            logger.info(
                "Summary: Negotiated agreements with %s providers", len(self.confirmed_agreements)
            )
            for provider_name, tasks in self.tasks_computed.items():
                logger.info("Summary: Provider '%s' computed %s tasks", provider_name, len(tasks))
