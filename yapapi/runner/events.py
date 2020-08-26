"""Representing and logging events in Golem computation."""
import asyncio
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import AsyncContextManager, Any, Mapping, Optional, TypeVar, Union

from typing_extensions import Protocol


class EventType(Enum):
    """Types of computation events."""

    # Proposals
    SUBSCRIPTION_CREATED = auto()
    SUBSCRIPTION_FAILED = auto()
    SUBSCRIPTION_COLLECT_FAILED = auto()
    PROPOSAL_BUFFERED = auto()
    PROPOSAL_FAILED = auto()
    PROPOSAL_RECEIVED = auto()
    PROPOSAL_REJECTED = auto()
    PROPOSAL_RESPONDED = auto()
    # Agreements
    AGREEMENT_CREATED = auto()
    AGREEMENT_CONFIRMED = auto()
    AGREEMENT_REJECTED = auto()
    PAYMENT_ACCEPTED = auto()
    PAYMENT_PREPARED = auto()
    PAYMENT_QUEUED = auto()
    # Activities
    ACTIVITY_CREATE_FAILED = auto()
    ACTIVITY_CREATED = auto()
    # Workers
    WORKER_CREATED = auto()
    WORKER_GET_WORK = auto()
    BATCH_SENT = auto()
    BATCH_STEP = auto()
    BATCH_GET_RESULTS = auto()
    BATCH_DONE = auto()
    DOWNLOAD_STARTED = auto()
    DOWNLOAD_FINISHED = auto()
    WORKER_DONE = auto()
    # Tasks
    TASK_ACCEPTED = auto()
    TASK_REJECTED = auto()


ET = EventType

_event_type_to_string: Mapping[EventType, str] = {
     ET.SUBSCRIPTION_CREATED: "Proposal subscription created",
     ET.SUBSCRIPTION_FAILED: "Failed to subscribe to proposals",
     ET.SUBSCRIPTION_COLLECT_FAILED: "Failed to collect proposals",
     ET.PROPOSAL_BUFFERED: "Proposal buffered",
     ET.PROPOSAL_FAILED: "Proposal failed",
     ET.PROPOSAL_RECEIVED: "Proposal received",
     ET.PROPOSAL_REJECTED: "Proposal rejected",
     ET.PROPOSAL_RESPONDED: "Responded to proposal",
     ET.AGREEMENT_CREATED: "Agreement created",
     ET.AGREEMENT_CONFIRMED: "Agreement confirmed",
     ET.AGREEMENT_REJECTED: "Agreement rejected",
     ET.PAYMENT_ACCEPTED: "Payment accepted",
     ET.PAYMENT_PREPARED: "Payment prepared",
     ET.PAYMENT_QUEUED: "Payment queued",
     ET.ACTIVITY_CREATE_FAILED: "Failed to create activity for agreement",
     ET.ACTIVITY_CREATED: "Activity created",
     ET.WORKER_CREATED: "Worker created",
     ET.WORKER_GET_WORK: "Getting work item",
     ET.BATCH_SENT: "Batch script sent",
     ET.BATCH_STEP: "Batch step executed",
     ET.BATCH_GET_RESULTS: "Getting batch results",
     ET.BATCH_DONE: "Batch done",
     ET.DOWNLOAD_STARTED: "Download started",
     ET.DOWNLOAD_FINISHED: "Download finished",
     ET.WORKER_DONE: "Worker done",
     ET.TASK_ACCEPTED: "Task accepted",
     ET.TASK_REJECTED: "Task rejected",
}

assert all(type_ in _event_type_to_string for type_ in EventType)


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


class AsyncEventBuffer(AsyncContextManager):
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
