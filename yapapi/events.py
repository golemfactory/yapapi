"""Representing events in Golem computation."""
import dataclasses
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from types import TracebackType
from typing import Any, Optional, Type, Tuple, List, TYPE_CHECKING


if TYPE_CHECKING:
    from yapapi.engine import Job
    from yapapi.rest.market import Subscription, OfferProposal, Agreement
    from yapapi.rest.payment import DebitNote, Invoice
    from yapapi.rest.activity import Activity
    from yapapi.executor.task import Task
    from yapapi.script import Script
    from yapapi.script.command import Command

ExcInfo = Tuple[Type[BaseException], BaseException, Optional[TracebackType]]


logger = logging.getLogger(__name__)


#   GENERAL TODO
#   *   Abstract dataclassess - how to? Is (init=False) enough?
#   *   exc_info & related interface

@dataclass(init=False)
class Event:
    """An abstract base class for types of events emitted by `Executor.submit()`."""
    timestamp: dataclasses.field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.now()

    def __init__(self):
        raise NotImplementedError()

    def extract_exc_info(self) -> Tuple[Optional[ExcInfo], "Event"]:
        """Extract exception information from this event.

        Return the extracted exception information
        and a copy of the event without the exception information.
        """
        return None, self


@dataclass(init=False)
class HasExcInfo(Event):
    """A base class for types of events that carry an optional exception info."""

    exc_info: Optional[ExcInfo] = None

    def extract_exc_info(self) -> Tuple[Optional[ExcInfo], "Event"]:
        """Return the `exc_info` field and a copy of this event with the field set to `None`."""

        exc_info = self.exc_info
        me = dataclasses.replace(self, exc_info=None)
        return exc_info, me


@dataclass(init=False)
class JobEvent(Event):
    job: 'Job'


@dataclass
class ComputationStarted(JobEvent):
    expires: datetime


@dataclass
class ComputationFinished(HasExcInfo, JobEvent):
    """Indicates successful completion if `exc_info` is `None` and a failure otherwise."""


@dataclass
class SubscriptionCreated(JobEvent):
    subscription: 'Subscription'


@dataclass
class SubscriptionFailed(JobEvent):
    reason: str


@dataclass
class CollectFailed(Event):
    reason: str


@dataclass(init=False)
class ProposalEvent(JobEvent):
    proposal: 'OfferProposal'


@dataclass
class ProposalReceived(ProposalEvent):
    pass


@dataclass
class ProposalRejected(ProposalEvent):
    reason: str


@dataclass
class ProposalResponded(ProposalEvent):
    pass


@dataclass
class ProposalConfirmed(ProposalEvent):
    pass


@dataclass
class ProposalFailed(HasExcInfo, ProposalEvent):
    pass


@dataclass
class NoProposalsConfirmed(Event):
    num_offers: int
    timeout: timedelta


@dataclass(init=False)
class AgreementEvent(JobEvent):
    agreement: 'Agreement'


@dataclass
class AgreementCreated(AgreementEvent):
    pass


@dataclass
class AgreementConfirmed(AgreementEvent):
    pass


@dataclass
class AgreementRejected(AgreementEvent):
    pass


@dataclass
class AgreementTerminated(AgreementEvent):
    reason: dict


@dataclass
class DebitNoteReceived(AgreementEvent):
    debit_note: 'DebitNote'


@dataclass
class PaymentAccepted(AgreementEvent):
    #   TODO: split to DebitNoteAccepted and InvoiceAccepted
    #   with DebitNote/Invoice
    pass


@dataclass
class PaymentPrepared(AgreementEvent):
    pass


@dataclass
class PaymentQueued(AgreementEvent):
    pass


@dataclass
class PaymentFailed(HasExcInfo, AgreementEvent):
    pass


@dataclass
class InvoiceReceived(AgreementEvent):
    invoice: 'Invoice'


@dataclass
class WorkerStarted(AgreementEvent):
    pass


@dataclass
class ActivityCreated(AgreementEvent):
    activity: 'Activity'


@dataclass
class ActivityCreateFailed(HasExcInfo, AgreementEvent):
    pass


@dataclass(init=False)
class TaskEvent(AgreementEvent):
    task: 'Task'


@dataclass
class TaskStarted(TaskEvent):
    task_data: Any


@dataclass
class TaskFinished(TaskEvent):
    pass


@dataclass
class WorkerFinished(HasExcInfo, AgreementEvent):
    """Indicates successful completion if `exc_info` is `None` and a failure otherwise."""


@dataclass(init=False)
class ScriptEvent(AgreementEvent):
    script: 'Script'


@dataclass
class ScriptSent(ScriptEvent):
    pass


@dataclass
class GettingResults(ScriptEvent):
    pass


@dataclass
class ScriptFinished(ScriptEvent):
    pass


@dataclass
class CommandEvent(ScriptEvent):
    command: 'Command'


@dataclass
class CommandExecuted(CommandEvent):
    success: bool = dataclasses.field(default=True)
    message: Optional[str] = dataclasses.field(default=None)
    stdout: Optional[str] = dataclasses.field(default=None)
    stderr: Optional[str] = dataclasses.field(default=None)


@dataclass
class CommandStarted(CommandEvent):
    pass


@dataclass
class CommandStdOut(CommandEvent):
    output: str


@dataclass
class CommandStdErr(CommandEvent):
    output: str


@dataclass
class TaskAccepted(TaskEvent):
    result: Any


@dataclass
class TaskRejected(TaskEvent):
    reason: str


@dataclass
class DownloadStarted(CommandEvent):
    path: str


@dataclass
class DownloadFinished(CommandEvent):
    path: str


@dataclass
class ShutdownFinished(HasExcInfo):
    """Indicates the completion of Executor shutdown sequence"""


@dataclass
class ExecutionInterrupted(HasExcInfo):
    """Emitted when Golem was stopped by an unhandled exception in code not managed by yapapi"""


@dataclass
class CommandEventContext:
    evt_cls: Type[CommandEvent]
    kwargs: dict

    def computation_finished(self, last_idx: int) -> bool:
        return self.evt_cls is CommandExecuted and (
            self.kwargs["cmd_idx"] >= last_idx or not self.kwargs["success"]
        )

    def event(self, job_id: str, agr_id: str, script_id: str, cmds: List) -> CommandEvent:
        kwargs = dict(job_id=job_id, agr_id=agr_id, script_id=script_id, **self.kwargs)
        if self.evt_cls is CommandExecuted:
            kwargs["command"] = cmds[self.kwargs["cmd_idx"]]
        return self.evt_cls(**kwargs)
