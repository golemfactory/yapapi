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
    from yapapi.executor.task import Task, TaskData, TaskResult
    from yapapi.rest.activity import Activity
    from yapapi.rest.market import Agreement
    from yapapi.rest.payment import DebitNote, Invoice
    from yapapi.engine import Job


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
    job: "Job"

    @property
    def job_id(self) -> str:
        return self.job.id

    @property
    def expires(self) -> datetime:
        return self.job.expiration_time

    @property
    def num_offers(self) -> int:
        return self.job.offers_collected


@attr.s(auto_attribs=True)
class ProposalEvent(JobEvent, abc.ABC):
    prop_id: str


@attr.s(auto_attribs=True)
class AgreementEvent(JobEvent, abc.ABC):
    agreement: "Agreement"

    @property
    def agr_id(self) -> str:
        return self.agreement.id

    @property
    def provider_id(self) -> str:
        return self.agreement.cached_details.raw_details.offer.provider_id  # type: ignore

    @property
    def provider_info(self) -> "NodeInfo":
        return self.agreement.cached_details.provider_node_info


@attr.s(auto_attribs=True)
class ActivityEvent(AgreementEvent, abc.ABC):
    activity: "Activity"


@attr.s(auto_attribs=True)
class TaskEvent(ActivityEvent, abc.ABC):
    task: "Task"

    @property
    def task_id(self) -> str:
        return self.task.id

    @property
    def task_data(self) -> "TaskData":
        return self.task.data


@attr.s(auto_attribs=True)
class ServiceEvent(ActivityEvent, abc.ABC):
    service: "Service"


@attr.s(auto_attribs=True)
class ScriptEvent(ActivityEvent, abc.ABC):
    script_id: Optional[str]


@attr.s(auto_attribs=True)
class CommandEvent(ScriptEvent, abc.ABC):
    cmd_idx: int


@attr.s(auto_attribs=True)
class InvoiceEvent(AgreementEvent, abc.ABC):
    invoice: "Invoice"

    @property
    def amount(self) -> str:
        return self.invoice.amount


@attr.s(auto_attribs=True)
class DebitNoteEvent(AgreementEvent, abc.ABC):
    debit_note: "DebitNote"

    @property
    def amount(self) -> str:
        return self.debit_note.total_amount_due


#   REAL EVENTS
@attr.s(auto_attribs=True)
class ComputationStarted(JobEvent):
    pass


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
class NoProposalsConfirmed(JobEvent):
    timeout: timedelta


@attr.s(auto_attribs=True)
class AgreementCreated(AgreementEvent):
    pass


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
class DebitNoteReceived(DebitNoteEvent):
    pass


@attr.s(auto_attribs=True)
class DebitNoteAccepted(DebitNoteEvent):
    pass


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
class InvoiceReceived(InvoiceEvent):
    pass


@attr.s(auto_attribs=True)
class InvoiceAccepted(InvoiceEvent):
    pass


@attr.s(auto_attribs=True)
class WorkerStarted(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class ActivityCreated(ActivityEvent):
    pass


@attr.s(auto_attribs=True)
class ActivityCreateFailed(AgreementEvent):
    pass


@attr.s(auto_attribs=True)
class TaskStarted(TaskEvent):
    pass


@attr.s(auto_attribs=True)
class TaskFinished(TaskEvent):
    pass


class ServiceStarted(ServiceEvent):
    """Work started for the given service object"""


class ServiceFinished(ServiceEvent):
    """Work finished for the given service object"""


class WorkerFinished(ActivityEvent):
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
    pass


@attr.s(auto_attribs=True)
class CommandStdOut(CommandEvent):
    output: str


@attr.s(auto_attribs=True)
class CommandStdErr(CommandEvent):
    output: str


@attr.s(auto_attribs=True)
class TaskAccepted(TaskEvent):
    @property
    def result(self) -> "TaskResult":
        assert self.task._result is not None
        return self.task._result


@attr.s(auto_attribs=True)
class TaskRejected(TaskEvent):
    reason: Optional[str]


#   TODO: currently it's hard to have a CommandEvent here, but it should be possible later
@attr.s(auto_attribs=True)
class DownloadStarted(ScriptEvent):
    path: str


#   TODO: ditto
@attr.s(auto_attribs=True)
class DownloadFinished(ScriptEvent):
    path: str


class ShutdownFinished(Event):
    """Indicates the completion of Executor shutdown sequence"""


class ExecutionInterrupted(Event):
    """Emitted when Golem was stopped by an unhandled exception in code not managed by yapapi"""
