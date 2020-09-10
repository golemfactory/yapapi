"""Representing and logging events in Golem computation."""
from enum import Enum, auto
import logging
import sys
from typing import Any, Optional, TypeVar, Union

from typing_extensions import Protocol

#if sys.version_info >= (3, 8):
#    from typing import get_args as get_type_args
#else:
#    from typing_extensions import get_args as get_type_args  # type: ignore


class _EventStrMixin:
    """Provides `__str()__` method for event types."""

    def __str__(self) -> str:
        return _event_type_to_string[self]


class SubscriptionEvent(_EventStrMixin, Enum):
    """Types of events related to subscriptions."""

    CREATED = auto()
    FAILED = auto()
    COLLECT_FAILED = auto()


class ProposalEvent(_EventStrMixin, Enum):
    """Types of events related to proposals."""

    BUFFERED = auto()
    FAILED = auto()
    RECEIVED = auto()
    REJECTED = auto()
    RESPONDED = auto()


class AgreementEvent(_EventStrMixin, Enum):
    """Types of events related to agreements."""

    CREATED = auto()
    CONFIRMED = auto()
    REJECTED = auto()
    PAYMENT_ACCEPTED = auto()
    PAYMENT_PREPARED = auto()
    PAYMENT_QUEUED = auto()
    INVOICE_RECEIVED = auto()


class WorkerEvent(_EventStrMixin, Enum):
    """Types of events related to workers."""

    CREATED = auto()
    ACTIVITY_CREATED = auto()
    ACTIVITY_CREATE_FAILED = auto()
    GOT_TASK = auto()
    FINISHED = auto()


class TaskEvent(_EventStrMixin, Enum):
    """Types of events related to tasks."""

    SCRIPT_SENT = auto()
    COMMAND_EXECUTED = auto()
    GETTING_RESULTS = auto()
    SCRIPT_FINISHED = auto()
    ACCEPTED = auto()
    REJECTED = auto()


class StorageEvent(_EventStrMixin, Enum):
    """Types of events related to storage."""

    DOWNLOAD_STARTED = auto()
    DOWNLOAD_FINISHED = auto()


EventType = Union[
    SubscriptionEvent, ProposalEvent, AgreementEvent, WorkerEvent, TaskEvent, StorageEvent
]


_event_type_to_string = {
    SubscriptionEvent.CREATED: "Proposal subscription created",
    SubscriptionEvent.FAILED: "Failed to subscribe to proposals",
    SubscriptionEvent.COLLECT_FAILED: "Failed to collect proposals",
    ProposalEvent.BUFFERED: "Proposal buffered",
    ProposalEvent.FAILED: "Proposal failed",
    ProposalEvent.RECEIVED: "Proposal received",
    ProposalEvent.REJECTED: "Proposal rejected",
    ProposalEvent.RESPONDED: "Responded to proposal",
    AgreementEvent.CREATED: "Agreement created",
    AgreementEvent.CONFIRMED: "Agreement confirmed",
    AgreementEvent.REJECTED: "Agreement rejected",
    AgreementEvent.PAYMENT_ACCEPTED: "Payment accepted for agreement",
    AgreementEvent.PAYMENT_PREPARED: "Payment prepared for agreement",
    AgreementEvent.PAYMENT_QUEUED: "Payment queued for agreement",
    AgreementEvent.INVOICE_RECEIVED: "Invoice received for agreement",
    WorkerEvent.CREATED: "Worker created",
    WorkerEvent.ACTIVITY_CREATED: "Activity created",
    WorkerEvent.ACTIVITY_CREATE_FAILED: "Failed to create activity",
    WorkerEvent.GOT_TASK: "Worker got new task",
    WorkerEvent.FINISHED: "Worker has finished",
    TaskEvent.SCRIPT_SENT: "Script sent to provider",
    TaskEvent.COMMAND_EXECUTED: "Command executed",
    TaskEvent.GETTING_RESULTS: "Getting task results",
    TaskEvent.SCRIPT_FINISHED: "Script finished",
    TaskEvent.ACCEPTED: "Task accepted",
    TaskEvent.REJECTED: "Task rejected",
    StorageEvent.DOWNLOAD_STARTED: "Download started",
    StorageEvent.DOWNLOAD_FINISHED: "Download finished",
}

#_all_event_types = {type_ for enum_ in get_type_args(EventType) for type_ in enum_}

#assert _all_event_types.issubset(_event_type_to_string.keys()), _all_event_types.difference(
#    _event_type_to_string.keys()
#)


ResourceId = Union[int, str]


E = TypeVar("E", contravariant=True, bound=EventType)


class EventEmitter(Protocol[E]):
    """A protocol for callables that can emit events of type `E`."""

    def __call__(
        self, event_type: E, resource_id: Optional[ResourceId] = None, **kwargs: Any
    ) -> None:
        """Emit an event with given event type and data."""


logger = logging.getLogger("yapapi.runner")


def log_event(
    event_type: EventType, resource_id: Optional[ResourceId] = None, **kwargs: Any,
) -> None:
    """Log an event. This function is compatible with the `EventEmitter` protocol."""

    def _format(obj: Any, max_len: int = 200) -> str:
        # This will also escape control characters, in particular,
        # newline characters in `obj` will be replaced by r"\n".
        text = repr(obj)
        if len(text) > max_len:
            text = text[: max_len - 3] + "..."
        return text

    if not logger.isEnabledFor(logging.INFO):
        return

    msg = _event_type_to_string[event_type]
    if resource_id is not None:
        msg += f", id = {_format(resource_id)}"
    if kwargs:
        msg += ", "
        msg += ", ".join(f"{arg} = {_format(value)}" for arg, value in kwargs.items())
    logger.info(msg)
