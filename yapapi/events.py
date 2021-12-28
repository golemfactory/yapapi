"""Representing events in Golem computation."""
import attr
import abc
from datetime import datetime, timedelta
import logging
from types import TracebackType
from typing import Any, Optional, Type, Tuple, TYPE_CHECKING

from yapapi.props import NodeInfo

ExcInfo = Tuple[Type[BaseException], BaseException, Optional[TracebackType]]

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from yapapi.services import Service
    from yapapi.script import Command


#   ABSTRACT EVENTS
@attr.s
class Event(abc.ABC):
    """An abstract base class for types of events emitted by `Executor.submit()`."""

    exc_info: Optional[ExcInfo] = attr.ib(default=None, kw_only=True)
    """Tuple containing exception info as returned by `sys.exc_info()`, if applicable."""

    @property
    def exception(self) -> Optional[BaseException]:
        """Exception associated with this event or `None` if the event doesn't mean a failure."""
        if self.exc_info:
            return self.exc_info[1]
        return None


@attr.s(auto_attribs=True)
class JobEvent(Event, abc.ABC):
    job_id: str


@attr.s(auto_attribs=True)
class ProposalEvent(JobEvent, abc.ABC):
    prop_id: str


@attr.s(auto_attribs=True)
class AgreementEvent(JobEvent, abc.ABC):
    agr_id: str


@attr.s(auto_attribs=True)
class TaskEvent(Event, abc.ABC):
    task_id: str


@attr.s(auto_attribs=True)
class ServiceEvent(AgreementEvent, abc.ABC):
    service: "Service"


@attr.s(auto_attribs=True)
class ScriptEvent(AgreementEvent, abc.ABC):
    script_id: Optional[str]


@attr.s(auto_attribs=True)
class CommandEvent(ScriptEvent, abc.ABC):
    cmd_idx: int


#   REAL EVENTS
@attr.s(auto_attribs=True)
class ComputationStarted(JobEvent):
    expires: datetime


class ComputationFinished(JobEvent):
    """Indicates successful completion if `exception` is `None` and a failure otherwise."""


@attr.s(auto_attribs=True)
class SubscriptionCreated(JobEvent):
    sub_id: str


@attr.s(auto_attribs=True)
class SubscriptionFailed(JobEvent):
    reason: str


@attr.s(auto_attribs=True)
class CollectFailed(Event):
    sub_id: str
    reason: str


@attr.s(auto_attribs=True)
class ProposalReceived(ProposalEvent):
    provider_id: str


@attr.s(auto_attribs=True)
class ProposalRejected(ProposalEvent):
    reason: Optional[str] = None


@attr.s(auto_attribs=True)
class ProposalResponded(ProposalEvent):
    pass


@attr.s(auto_attribs=True)
class ProposalConfirmed(ProposalEvent):
    pass


@attr.s(auto_attribs=True)
class ProposalFailed(ProposalEvent):
    pass


@attr.s(auto_attribs=True)
class NoProposalsConfirmed(Event):
    num_offers: int
    timeout: timedelta


@attr.s(auto_attribs=True)
class AgreementCreated(AgreementEvent):
    provider_id: str
    provider_info: NodeInfo


@attr.s(auto_attribs=True)
class AgreementConfirmed(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class AgreementRejected(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class AgreementTerminated(AgreementEvent):
    reason: dict


@attr.s(auto_attribs=True)
class DebitNoteReceived(AgreementEvent):
    note_id: str
    amount: str


@attr.s(auto_attribs=True)
class DebitNoteAccepted(AgreementEvent):
    note_id: str
    amount: str


@attr.s(auto_attribs=True)
class PaymentPrepared(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class PaymentQueued(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class PaymentFailed(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class InvoiceReceived(AgreementEvent):
    inv_id: str
    amount: str


@attr.s(auto_attribs=True)
class InvoiceAccepted(AgreementEvent):
    inv_id: str
    amount: str


@attr.s(auto_attribs=True)
class WorkerStarted(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class ActivityCreated(AgreementEvent):
    act_id: str


@attr.s(auto_attribs=True)
class ActivityCreateFailed(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class TaskStarted(AgreementEvent, TaskEvent):
    task_data: Any


@attr.s(auto_attribs=True)
class TaskFinished(AgreementEvent, TaskEvent):
    pass


class ServiceStarted(ServiceEvent):
    """Work started for the given service object"""


class ServiceFinished(ServiceEvent):
    """Work finished for the given service object"""


class WorkerFinished(AgreementEvent):
    """Indicates successful completion if `exception` is `None` and a failure otherwise."""


@attr.s(auto_attribs=True)
class ScriptSent(ScriptEvent):
    cmds: Any


@attr.s(auto_attribs=True)
class GettingResults(ScriptEvent):
    pass


@attr.s(auto_attribs=True)
class ScriptFinished(ScriptEvent):
    pass


@attr.s
class CommandExecuted(CommandEvent):
    command: "Command" = attr.ib()
    success: bool = attr.ib(default=True)
    message: Optional[str] = attr.ib(default=None)
    stdout: Optional[str] = attr.ib(default=None)
    stderr: Optional[str] = attr.ib(default=None)


@attr.s(auto_attribs=True)
class CommandStarted(CommandEvent):
    command: "Command"


@attr.s(auto_attribs=True)
class CommandStdOut(CommandEvent):
    output: str


@attr.s(auto_attribs=True)
class CommandStdErr(CommandEvent):
    output: str


@attr.s(auto_attribs=True)
class TaskAccepted(TaskEvent):
    result: Any


@attr.s(auto_attribs=True)
class TaskRejected(TaskEvent):
    reason: Optional[str]


@attr.s(auto_attribs=True)
class DownloadStarted(Event):
    path: str


@attr.s(auto_attribs=True)
class DownloadFinished(Event):
    path: str


class ShutdownFinished(Event):
    """Indicates the completion of Executor shutdown sequence"""


class ExecutionInterrupted(Event):
    """Emitted when Golem was stopped by an unhandled exception in code not managed by yapapi"""
