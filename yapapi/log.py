from itertools import count
import json
import logging
from typing import Any, Dict, Iterator, Set

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
    logger.debug("%s %s", type(event).__name__, json.dumps(info))


# TODO: consider moving this class to examples/
class SummaryLogger:
    """Aggregates information from computation events to provide a high-level description.

    A separate instance of this class should be used for each call of `Engine.map()`.
    """

    def __init__(self):
        # Generates subsequent numbers, for use in generated provider names
        self.numbers: Iterator[int] = count(1)
        # Set of proposal issuer ids
        self.proposal_issuers: Set[str] = set()
        # Maps agreement ids to provider names
        self.sent_agreements: Dict[str, str] = {}
        # Maps task id to task data
        self.task_data: Dict[str, Any] = {}
        # Maps worker id to agreement id
        self.worker_agreement: Dict[str, str] = {}

    def log(self, event: EventType) -> None:

        if isinstance(event, Event.SubscriptionCreated):
            logger.info(event_type_to_string[type(event)])

        elif isinstance(event, Event.ProposalReceived):
            self.proposal_issuers.add(event.provider_id)

        elif isinstance(event, Event.ProposalConfirmed):
            logger.info(
                "Received proposals from %s providers so far",
                len(self.proposal_issuers)
            )

        elif isinstance(event, Event.AgreementCreated):
            provider_name = event.provider_id.name
            if not provider_name:
                provider_name = f"provider-{next(self.numbers)}"
            logger.info("Agreement proposed to provider %s", provider_name)
            self.sent_agreements[event.agr_id] = provider_name

        elif isinstance(event, Event.AgreementConfirmed):
            assert event.agr_id in self.sent_agreements
            logger.info("Agreement confirmed by provider %s", self.sent_agreements[event.agr_id])

        elif isinstance(event, Event.WorkerCreated):
            self.worker_agreement[event.wrk_id] = event.agr_id

        elif isinstance(event, Event.TaskStarted):
            self.task_data[event.task_id] = event.task_data

        elif isinstance(event, Event.ScriptSent):
            assert event.task_id in self.task_data
            assert event.wrk_id in self.worker_agreement
            agr_id = self.worker_agreement[event.wrk_id]
            assert agr_id in self.sent_agreements
            provider_name = self.sent_agreements[agr_id]
            logger.info(
                "Task sent to provider %s, task data: %s",
                provider_name,
                self.task_data(event.task_id)
            )

# def log_highlevel(event: EventType) -> None:
#
