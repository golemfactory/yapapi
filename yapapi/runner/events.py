"""Representing and logging events in Golem computation."""
import asyncio
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import AsyncContextManager, Any, Mapping, Optional, TypeVar, Union
import sys

from typing_extensions import Protocol

if sys.version_info >= (3, 8):
    from typing import get_args as get_type_args
else:
    from typing_extensions import get_args as get_type_args  # type: ignore


class _EventBase:
    """Provides `__str()__` method for event types."""
    def __str__(self) -> str:
        return _event_type_to_string[self]


class SubscriptionEvent(_EventBase, Enum):
    """Types of events related to subscriptions."""
    CREATED = auto()
    FAILED = auto()
    COLLECT_FAILED = auto()


class ProposalEvent(_EventBase, Enum):
    """Types of events related to proposals."""
    BUFFERED = auto()
    FAILED = auto()
    RECEIVED = auto()
    REJECTED = auto()
    RESPONDED = auto()


class AgreementEvent(_EventBase, Enum):
    """Types of events related to agreements."""
    CREATED = auto()
    CONFIRMED = auto()
    REJECTED = auto()
    PAYMENT_ACCEPTED = auto()
    PAYMENT_PREPARED = auto()
    PAYMENT_QUEUED = auto()


class WorkerEvent(_EventBase, Enum):
    """Types of events related to workers."""
    CREATED = auto()
    ACTIVITY_CREATED = auto()
    ACTIVITY_CREATE_FAILED = auto()
    GOT_TASK = auto()
    FINISHED = auto()


class TaskEvent(_EventBase, Enum):
    """Types of events related to tasks."""
    SCRIPT_SENT = auto()
    COMMAND_EXECUTED = auto()
    GETTING_RESULTS = auto()
    SCRIPT_FINISHED = auto()
    ACCEPTED = auto()
    REJECTED = auto()


class StorageEvent(_EventBase, Enum):
    """Types of events related to storage."""
    DOWNLOAD_STARTED = auto()
    DOWNLOAD_FINISHED = auto()


EventType = Union[
    SubscriptionEvent,
    ProposalEvent,
    AgreementEvent,
    WorkerEvent,
    TaskEvent,
    StorageEvent
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
    AgreementEvent.PAYMENT_ACCEPTED: "Payment accepted",
    AgreementEvent.PAYMENT_PREPARED: "Payment prepared",
    AgreementEvent.PAYMENT_QUEUED: "Payment queued",
    WorkerEvent.CREATED: "Worker created",
    WorkerEvent.ACTIVITY_CREATED: "Activity created",
    WorkerEvent.ACTIVITY_CREATE_FAILED: "Failed to create activity",
    WorkerEvent.GOT_TASK: "Task assigned to worker",
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

_all_event_types = {
    type_
    for enum_ in get_type_args(EventType)
    for type_ in enum_
}

assert _all_event_types.issubset(_event_type_to_string.keys()), (
    _all_event_types.difference(_event_type_to_string.keys())
)


ResourceId = Union[int, str]


@dataclass(frozen=True)
class _Event:
    """Represents an event in a Golem computation."""

    event_type: EventType
    resource_id: Optional[ResourceId] = None
    info: Mapping[str, Any] = field(default_factory=dict)


E = TypeVar("E", contravariant=True, bound=EventType)


class EventEmitter(Protocol[E]):
    """A protocol for callables that can emit events of type `E`."""

    def __call__(
        self,
        event_type: E,
        resource_id: Optional[ResourceId] = None,
        **kwargs: Any
    ) -> None:
        """Emit an event with given event type and data."""


def log_event(
    event_type: EventType,
    resource_id: Optional[ResourceId] = None,
    **kwargs: Any,
) -> None:
    """Log an event."""

    # TODO: use the `logging` module instead of `print()`
    print(f"{_event_type_to_string[event_type]}, id = {resource_id}, info = {kwargs}")


class _AsyncEventBuffer(AsyncContextManager):
    """Wraps a given event emitter to provide buffering.

    The `emitter` method is an event emitter that buffers the events
    and emits them asynchronously later using the wrapped emitter.
    """

    _wrapped_emitter: EventEmitter
    _event_queue: "asyncio.Queue[_Event]"
    _task: Optional[asyncio.Task]

    def __init__(self, wrapped_emitter: EventEmitter):
        self._wrapped_emitter = wrapped_emitter
        self._event_queue = asyncio.Queue()
        self._task = None

    async def _worker(self) -> None:
        while True:
            event = await self._event_queue.get()
            self._wrapped_emitter(
                event.event_type, event.resource_id, **event.info
            )

    async def __aenter__(self):
        self._task = asyncio.create_task(self._worker())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._task.cancel()

    def emitter(
        self,
        event_type: EventType,
        resource_id: Optional[ResourceId] = None,
        **kwargs: Any
    ) -> None:
        """Buffer the event to be emitted asynchronously by the wrapped emitter."""
        self._event_queue.put_nowait(_Event(event_type, resource_id, kwargs))
