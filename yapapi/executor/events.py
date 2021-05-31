"""Representing events in Golem computation."""
import dataclasses
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from types import TracebackType
from typing import Any, Optional, Type, Tuple, List

from yapapi.props import NodeInfo

ExcInfo = Tuple[Type[BaseException], BaseException, Optional[TracebackType]]


logger = logging.getLogger(__name__)


@dataclass(init=False)
class Event:
    """An abstract base class for types of events emitted by `Executor.submit()`."""

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
class ComputationEvent(Event):
    job_id: str


@dataclass
class ComputationStarted(ComputationEvent):
    expires: datetime


@dataclass
class ComputationFinished(HasExcInfo, ComputationEvent):
    """Indicates successful completion if `exc_info` is `None` and a failure otherwise."""


@dataclass
class SubscriptionCreated(Event):
    sub_id: str


@dataclass
class SubscriptionFailed(Event):
    reason: str


@dataclass
class CollectFailed(Event):
    sub_id: str
    reason: str


@dataclass(init=False)
class ProposalEvent(Event):
    prop_id: str


@dataclass
class ProposalReceived(ProposalEvent):
    provider_id: str


@dataclass
class ProposalRejected(ProposalEvent):
    reason: Optional[str] = None


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
class AgreementEvent(Event):
    agr_id: str


@dataclass
class AgreementCreated(AgreementEvent):
    provider_id: str
    provider_info: NodeInfo


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
    note_id: str
    amount: str


@dataclass
class PaymentAccepted(AgreementEvent):
    inv_id: str
    amount: str


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
    inv_id: str
    amount: str


@dataclass
class WorkerStarted(AgreementEvent):
    pass


@dataclass
class ActivityCreated(AgreementEvent):
    act_id: str


@dataclass
class ActivityCreateFailed(HasExcInfo, AgreementEvent):
    pass


@dataclass(init=False)
class TaskEvent(Event):
    task_id: str


@dataclass
class TaskStarted(AgreementEvent, TaskEvent):
    task_data: Any


@dataclass
class TaskFinished(AgreementEvent, TaskEvent):
    pass


@dataclass
class WorkerFinished(HasExcInfo, AgreementEvent):
    """Indicates successful completion if `exc_info` is `None` and a failure otherwise."""


@dataclass(init=False)
class ScriptEvent(AgreementEvent):
    script_id: Optional[str]


@dataclass
class ScriptSent(ScriptEvent):
    cmds: Any


@dataclass
class GettingResults(ScriptEvent):
    pass


@dataclass
class ScriptFinished(ScriptEvent):
    pass


@dataclass
class CommandEvent(ScriptEvent):
    cmd_idx: int


@dataclass
class CommandExecuted(CommandEvent):
    command: Any
    success: bool = dataclasses.field(default=True)
    message: Optional[str] = dataclasses.field(default=None)
    stdout: Optional[str] = dataclasses.field(default=None)
    stderr: Optional[str] = dataclasses.field(default=None)


@dataclass
class CommandStarted(CommandEvent):
    command: str


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
    reason: Optional[str]


@dataclass
class DownloadStarted(Event):
    path: str


@dataclass
class DownloadFinished(Event):
    path: str


@dataclass
class ShutdownFinished(HasExcInfo):
    """Indicates the completion of Executor shutdown sequence"""


@dataclass
class CommandEventContext:
    evt_cls: Type[CommandEvent]
    kwargs: dict

    def computation_finished(self, last_idx: int) -> bool:
        return self.evt_cls is CommandExecuted and (
            self.kwargs["cmd_idx"] >= last_idx or not self.kwargs["success"]
        )

    def event(self, agr_id: str, script_id: str, cmds: List) -> CommandEvent:
        kwargs = dict(agr_id=agr_id, script_id=script_id, **self.kwargs)
        if self.evt_cls is CommandExecuted:
            kwargs["command"] = cmds[self.kwargs["cmd_idx"]]
        return self.evt_cls(**kwargs)
