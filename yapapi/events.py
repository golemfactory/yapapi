"""Objects representing events in Golem computation.

Everytime something important happens, an event is emitted.
Emitted events are passed to all current event consumers, set
either in :func:`yapapi.Golem.__init__` or via :func:`yapapi.Golem.add_event_consumer`.

Every event has a set of attributes that describe it. Set of attributes shared by various events:

* exc_info - either `None`, or tuple returned by :func:`sys.exc_info()`
* job - :class:`yapapi.engine.Job` - [TODO]
* script - :class:`yapapi.script.Script` - [TODO]
* agreement - :class:`yapapi.rest.market.Agreement` - [TODO]
* [TODO]

Events should be consumed in strict `read_only` mode: event objects are shared between all event consumers,
and their attributes are used internally by the Golem engine, so any change might have unexpected
side effects.

Events inheritance tree
-----------------------

Notes:

*   Only leaf events are ever emitted, other events (names ending with "Event") are abstract classes
*   Every abstract class has one more `yapapi` object attached then the parent, e.g.

    *   `JobEvent` is an `Event` that is related to a given `yapapi.engine.Job`
    *   `AgreementEvent` is a `JobEvent` that is related to a given `yapapi.rest.market.Agreement`

::

    Event
        JobEvent
            SubscriptionFailed
            SubscriptionEvent
                SubscriptionCreated
                CollectFailed
            ProposalEvent
                ProposalReceived
                ProposalRejected
                ProposalResponded
                ProposalConfirmed
                ProposalFailed
            NoProposalsConfirmed
            JobStarted
            JobFinished
            AgreementEvent
                AgreementCreated
                AgreementConfirmed
                AgreementRejected
                AgreementTerminated
                ActivityCreateFailed
                WorkerStarted
                ActivityEvent
                    ActivityCreated
                    TaskEvent
                        TaskStarted
                        TaskFinished
                        TaskAccepted
                        TaskRejected
                    ServiceEvent
                        ServiceStarted
                        ServiceFinished
                    ScriptEvent
                        ScriptSent
                        CommandEvent
                            CommandStarted
                            CommandStdOut
                            CommandStdErr
                            CommandExecuted
                            DownloadStarted
                            DownloadFinished
                        GettingResults
                        ScriptFinished
                    WorkerFinished
                InvoiceEvent
                    InvoiceReceived
                    InvoiceAccepted
                DebitNoteEvent
                    DebitNoteReceived
                    DebitNoteAccepted
                PaymentPrepared
                PaymentQueued
                PaymentFailed
        ExecutionInterrupted
        ShutdownFinished
"""

import attr
import abc
from datetime import datetime, timedelta
import logging
from types import TracebackType
from typing import List, Optional, Type, Tuple, TypeVar

from yapapi.props import NodeInfo

#   Q: Why `import yapapi` here?
#   A: Because we want to have typing annotations without circular imports
#   Q: Why not `if TYPE_CHECKING:`?
#   A: Because TYPE_CHECKING doesn't work for a sphinx plugin we use (`sphinx-autodoc-typehints`)
#      https://github.com/tox-dev/sphinx-autodoc-typehints/issues/22
#      -> Compare e.g. "MarketStrategy" typing in `yapapi.golem.Golem.__init__`
import yapapi


logger = logging.getLogger(__name__)

ExcInfo = Tuple[Type[BaseException], BaseException, Optional[TracebackType]]

#   Types used in all `emit()` methods, to tell the typechecker that
#   they return object of the same class that was passed as an argument.
#   They are only few of them (e.g. there's no AgreementEventType) because the missing ones are not
#   needed (i.e. there are no emit() methods granular enough).
EventType = TypeVar("EventType", bound="Event")
JobEventType = TypeVar("JobEventType", bound="JobEvent")
ActivityEventType = TypeVar("ActivityEventType", bound="ActivityEvent")
TaskEventType = TypeVar("TaskEventType", bound="TaskEvent")
ScriptEventType = TypeVar("ScriptEventType", bound="ScriptEvent")
CommandEventType = TypeVar("CommandEventType", bound="CommandEvent")


#   ABSTRACT EVENTS
@attr.s(frozen=True, repr=False)
class Event(abc.ABC):
    """An abstract base class for types of events emitted by `Executor.submit()`."""

    exc_info: Optional[ExcInfo] = attr.ib(default=None, kw_only=True)
    """Tuple containing exception info as returned by `sys.exc_info()`, if applicable."""

    timestamp: datetime = attr.ib(default=datetime.now(), init=False)
    """Event creation time"""

    def __str__(self) -> str:
        """Mimics Python's default `repr` format, but excludes the fields `exc_info` and `timestamp` from it.

        If `exc_info` is not `None`, its underlying exception is included in the result string
        under the key `exception`.
        """
        fields: Tuple[attr.Attribute] = attr.fields(self.__class__)  # type: ignore
        field_reprs: List[str] = []

        for field in fields:
            field_value = getattr(self, field.name)

            if field.name == "exc_info":
                if field_value:
                    field_reprs.append(f"exception={repr(field_value[1])}")
            elif field.name == "timestamp":
                continue
            else:
                field_reprs.append(f"{field.name}={repr(field_value)}")

        return f"{self.__class__.__name__}({', '.join(field_reprs)})"

    def __repr__(self) -> str:
        return str(self)

    @property
    def exception(self) -> Optional[BaseException]:
        """Exception associated with this event or `None` if the event doesn't mean a failure."""
        if self.exc_info:
            return self.exc_info[1]
        return None


@attr.s(auto_attribs=True, repr=False)
class JobEvent(Event, abc.ABC):
    job: "yapapi.engine.Job"

    @property
    def job_id(self) -> str:
        return self.job.id


@attr.s(auto_attribs=True, repr=False)
class SubscriptionEvent(JobEvent, abc.ABC):
    subscription: "yapapi.rest.market.Subscription"


@attr.s(auto_attribs=True, repr=False)
class ProposalEvent(JobEvent, abc.ABC):
    proposal: "yapapi.rest.market.OfferProposal"

    @property
    def prop_id(self) -> str:
        return self.proposal.id

    @property
    def provider_id(self) -> str:
        return self.proposal.issuer


@attr.s(auto_attribs=True, repr=False)
class AgreementEvent(JobEvent, abc.ABC):
    agreement: "yapapi.rest.market.Agreement"

    @property
    def agr_id(self) -> str:
        return self.agreement.id

    @property
    def provider_id(self) -> str:
        return self.agreement.details.raw_details.offer.provider_id  # type: ignore

    @property
    def provider_info(self) -> "NodeInfo":
        return self.agreement.details.provider_node_info


@attr.s(auto_attribs=True, repr=False)
class ActivityEvent(AgreementEvent, abc.ABC):
    activity: "yapapi.rest.activity.Activity"


@attr.s(auto_attribs=True, repr=False)
class TaskEvent(ActivityEvent, abc.ABC):
    task: "yapapi.executor.task.Task"

    @property
    def task_id(self) -> str:
        return self.task.id

    @property
    def task_data(self) -> "yapapi.executor.task.TaskData":
        return self.task.data


@attr.s(auto_attribs=True, repr=False)
class ServiceEvent(ActivityEvent, abc.ABC):
    service: "yapapi.services.Service"


@attr.s(auto_attribs=True, repr=False)
class ScriptEvent(ActivityEvent, abc.ABC):
    script: "yapapi.script.Script"

    @property
    def script_id(self) -> int:
        return self.script.id

    @property
    def cmds(self) -> List["yapapi.script.command.BatchCommand"]:
        #   NOTE: This assumes `script._before()` was already called
        #         (currently this is always true)
        return self.script._evaluate()


@attr.s(auto_attribs=True, repr=False)
class CommandEvent(ScriptEvent, abc.ABC):
    command: "yapapi.script.command.Command"


@attr.s(auto_attribs=True, repr=False)
class InvoiceEvent(AgreementEvent, abc.ABC):
    invoice: "yapapi.rest.payment.Invoice"

    @property
    def amount(self) -> str:
        return self.invoice.amount


@attr.s(auto_attribs=True, repr=False)
class DebitNoteEvent(AgreementEvent, abc.ABC):
    debit_note: "yapapi.rest.payment.DebitNote"

    @property
    def amount(self) -> str:
        return self.debit_note.total_amount_due


#   REAL EVENTS
class JobStarted(JobEvent):
    pass


class JobFinished(JobEvent):
    """Indicates successful completion if `exception` is `None` and a failure otherwise."""


class SubscriptionCreated(SubscriptionEvent):
    pass


@attr.s(auto_attribs=True, repr=False)
class SubscriptionFailed(JobEvent):
    reason: str


@attr.s(auto_attribs=True, repr=False)
class CollectFailed(SubscriptionEvent):
    reason: str


class ProposalReceived(ProposalEvent):
    pass


@attr.s(auto_attribs=True, repr=False)
class ProposalRejected(ProposalEvent):
    reason: Optional[str] = None


class ProposalResponded(ProposalEvent):
    pass


class ProposalConfirmed(ProposalEvent):
    pass


class ProposalFailed(ProposalEvent):
    pass


@attr.s(auto_attribs=True, repr=False)
class NoProposalsConfirmed(JobEvent):
    timeout: timedelta


class AgreementCreated(AgreementEvent):
    pass


class AgreementConfirmed(AgreementEvent):
    pass


class AgreementRejected(AgreementEvent):
    pass


@attr.s(auto_attribs=True, repr=False)
class AgreementTerminated(AgreementEvent):
    reason: dict


class DebitNoteReceived(DebitNoteEvent):
    pass


class DebitNoteAccepted(DebitNoteEvent):
    pass


class PaymentPrepared(AgreementEvent):
    pass


class PaymentQueued(AgreementEvent):
    pass


class PaymentFailed(AgreementEvent):
    pass


class InvoiceReceived(InvoiceEvent):
    pass


class InvoiceAccepted(InvoiceEvent):
    pass


class WorkerStarted(AgreementEvent):
    pass


class ActivityCreated(ActivityEvent):
    pass


class ActivityCreateFailed(AgreementEvent):
    pass


class TaskStarted(TaskEvent):
    pass


class TaskFinished(TaskEvent):
    pass


class ServiceStarted(ServiceEvent):
    """Work started for the given service object"""


class ServiceFinished(ServiceEvent):
    """Work finished for the given service object"""


class WorkerFinished(ActivityEvent):
    """Indicates successful completion if `exception` is `None` and a failure otherwise."""


class ScriptSent(ScriptEvent):
    pass


class GettingResults(ScriptEvent):
    pass


class ScriptFinished(ScriptEvent):
    pass


@attr.s(auto_attribs=True, repr=False)
class CommandExecuted(CommandEvent):
    success: bool
    message: str
    stdout: Optional[str] = None
    stderr: Optional[str] = None


class CommandStarted(CommandEvent):
    pass


@attr.s(auto_attribs=True, repr=False)
class CommandStdOut(CommandEvent):
    output: str


@attr.s(auto_attribs=True, repr=False)
class CommandStdErr(CommandEvent):
    output: str


@attr.s(auto_attribs=True, repr=False)
class TaskAccepted(TaskEvent):
    @property
    def result(self) -> "yapapi.executor.task.TaskResult":
        assert self.task._result is not None
        return self.task._result


@attr.s(auto_attribs=True, repr=False)
class TaskRejected(TaskEvent):
    reason: Optional[str]


@attr.s(auto_attribs=True, repr=False)
class DownloadStarted(CommandEvent):
    command: "yapapi.script.command._ReceiveContent"

    @property
    def path(self) -> str:
        return self.command._src_path


@attr.s(auto_attribs=True, repr=False)
class DownloadFinished(CommandEvent):
    command: "yapapi.script.command._ReceiveContent"

    @property
    def path(self) -> str:
        return str(self.command._dst_path)


class ShutdownFinished(Event):
    """Indicates the completion of Executor shutdown sequence"""


class ExecutionInterrupted(Event):
    """Emitted when Golem was stopped by an unhandled exception in code not managed by yapapi"""
