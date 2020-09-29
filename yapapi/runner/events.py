"""Representing events in Golem computation."""
import sys
from dataclasses import dataclass
from typing import Any, Optional

from yapapi.props import Identification


@dataclass(init=False)
class Event:
    """An abstract base class for types of events emitted by `Engine.map()`."""

    def __init__(self):
        raise NotImplementedError()


@dataclass
class ComputationStarted(Event):
    pass


@dataclass
class ComputationFinished(Event):
    pass


@dataclass
class ComputationFailed(Event):
    reason: str


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
    reason: Optional[str]


@dataclass
class ProposalResponded(ProposalEvent):
    pass


@dataclass
class ProposalConfirmed(ProposalEvent):
    pass


@dataclass
class ProposalFailed(ProposalEvent):
    reason: str


@dataclass(init=False)
class AgreementEvent(Event):
    agr_id: str


@dataclass
class AgreementCreated(AgreementEvent):
    provider_id: Identification


@dataclass
class AgreementConfirmed(AgreementEvent):
    pass


@dataclass
class AgreementRejected(AgreementEvent):
    pass


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
class ActivityCreateFailed(AgreementEvent):
    pass


@dataclass(init=False)
class TaskEvent(Event):
    task_id: str


@dataclass
class TaskStarted(AgreementEvent, TaskEvent):
    task_data: str


@dataclass
class WorkerFinished(AgreementEvent):
    pass


@dataclass
class ScriptSent(AgreementEvent, TaskEvent):
    cmds: Any


@dataclass
class CommandExecuted(AgreementEvent, TaskEvent):
    cmd_idx: int
    message: str


@dataclass
class GettingResults(AgreementEvent, TaskEvent):
    pass


@dataclass
class ScriptFinished(AgreementEvent, TaskEvent):
    pass


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
