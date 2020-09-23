"""Representing events in Golem computation."""
from traceback import TracebackException
import sys
from typing import Any, NamedTuple, Optional, Union

if sys.version_info >= (3, 8):
    from typing import get_args as get_type_args
elif sys.version_info >= (3, 7):
    from typing_extensions import get_args as get_type_args  # type: ignore
else:
    get_type_args = None


from yapapi.props import Identification


class Event:
    """A namespace for event types."""

    ComputationStarted = NamedTuple("ComputationStarted", [])
    ComputationFinished = NamedTuple("ComputationFinished", [])
    ComputationFailed = NamedTuple("ComputationFailed", [("reason", str)])
    SubscriptionCreated = NamedTuple("SubscriptionCreated", [("sub_id", str)])
    SubscriptionFailed = NamedTuple("SubscriptionFailed", [("reason", str)])
    CollectFailed = NamedTuple("CollectFailed", [("sub_id", str), ("reason", str)])
    ProposalReceived = NamedTuple("ProposalReceived", [("prop_id", str), ("provider_id", str)])
    ProposalRejected = NamedTuple("ProposalRejected", [("prop_id", str), ("reason", Optional[str])])
    ProposalResponded = NamedTuple("ProposalResponded", [("prop_id", str)])
    ProposalConfirmed = NamedTuple("ProposalConfirmed", [("prop_id", str)])
    ProposalFailed = NamedTuple("ProposalFailed", [("prop_id", str), ("reason", str)])
    AgreementCreated = NamedTuple(
        "AgreementCreated", [("agr_id", str), ("provider_id", Identification)]
    )
    AgreementConfirmed = NamedTuple("AgreementConfirmed", [("agr_id", str)])
    AgreementRejected = NamedTuple("AgreementRejected", [("agr_id", str)])
    PaymentAccepted = NamedTuple(
        "PaymentAccepted", [("agr_id", str), ("inv_id", str), ("amount", str)]
    )
    PaymentPrepared = NamedTuple("PaymentPrepared", [("agr_id", str)])
    PaymentQueued = NamedTuple("PaymentQueued", [("agr_id", str)])
    InvoiceReceived = NamedTuple(
        "InvoiceReceived", [("agr_id", str), ("inv_id", str), ("amount", str)]
    )
    WorkerStarted = NamedTuple("WorkerStarted", [("agr_id", str)])
    ActivityCreated = NamedTuple("ActivityCreated", [("act_id", str), ("agr_id", str)])
    ActivityCreateFailed = NamedTuple("ActivityCreateFailed", [("agr_id", str)])
    TaskStarted = NamedTuple("TaskStarted", [("agr_id", str), ("task_id", str), ("task_data", Any)])
    WorkerFinished = NamedTuple("WorkerFinished", [("agr_id", str)])
    ScriptSent = NamedTuple("ScriptSent", [("agr_id", str), ("task_id", str), ("cmds", Any)])
    CommandExecuted = NamedTuple(
        "CommandExecuted", [("agr_id", str), ("task_id", str), ("cmd_idx", int), ("message", str)]
    )
    GettingResults = NamedTuple("GettingResults", [("agr_id", str), ("task_id", str)])
    ScriptFinished = NamedTuple("ScriptFinished", [("agr_id", str), ("task_id", str)])
    TaskAccepted = NamedTuple("TaskAccepted", [("task_id", str), ("result", Any)])
    TaskRejected = NamedTuple("TaskRejected", [("task_id", str), ("reason", Optional[str])])
    DownloadStarted = NamedTuple("DownloadStarted", [("path", str)])
    DownloadFinished = NamedTuple("DownloadFinished", [("path", str)])

    def __init__(self, *_args, **_kwargs):
        raise NotImplementedError(f"Cannot create instances of {type(self).__name__}")


EventType = Union[
    Event.ComputationStarted,
    Event.ComputationFinished,
    Event.ComputationFailed,
    Event.SubscriptionCreated,
    Event.SubscriptionFailed,
    Event.CollectFailed,
    Event.ProposalFailed,
    Event.ProposalReceived,
    Event.ProposalRejected,
    Event.ProposalResponded,
    Event.ProposalConfirmed,
    Event.AgreementCreated,
    Event.AgreementConfirmed,
    Event.AgreementRejected,
    Event.PaymentAccepted,
    Event.PaymentPrepared,
    Event.PaymentQueued,
    Event.InvoiceReceived,
    Event.WorkerStarted,
    Event.ActivityCreated,
    Event.ActivityCreateFailed,
    Event.TaskStarted,
    Event.WorkerFinished,
    Event.ScriptSent,
    Event.CommandExecuted,
    Event.GettingResults,
    Event.ScriptFinished,
    Event.TaskAccepted,
    Event.TaskRejected,
    Event.DownloadStarted,
    Event.DownloadFinished,
]


if get_type_args:
    # Check that `EventType` includes all event types from `Event`
    event_membertypes = set(value for value in vars(Event).values() if isinstance(value, type))
    eventtype_members = set(get_type_args(EventType))
    assert event_membertypes == eventtype_members, event_membertypes.difference(eventtype_members)


# Default human-readable representation of event types.
event_type_to_string = {
    Event.ComputationStarted: "Computation started",
    Event.ComputationFinished: "Computation finished",
    Event.ComputationFailed: "Computation failed",
    Event.SubscriptionCreated: "Demand published on the market",
    Event.SubscriptionFailed: "Demand publication failed",
    Event.CollectFailed: "Failed to collect proposals for demand",
    Event.ProposalReceived: "Proposal received from the market",
    Event.ProposalRejected: "Proposal rejected",  # by whom? alt: Rejected a proposal?
    Event.ProposalResponded: "Responded to a proposal",
    Event.ProposalFailed: "Failed to respond to proposal",
    Event.ProposalConfirmed: "Proposal confirmed by provider",  # Proposal negotiated with provider?
    Event.AgreementCreated: "Agreement proposal sent to provider",
    Event.AgreementConfirmed: "Agreement approved by provider",
    Event.AgreementRejected: "Agreement rejected by provider",
    Event.PaymentAccepted: "Payment accepted",  # by who?
    Event.PaymentPrepared: "Payment prepared",
    Event.PaymentQueued: "Payment queued",
    Event.InvoiceReceived: "Invoice received",  # by who?
    Event.WorkerStarted: "Worker started for agreement",
    Event.ActivityCreated: "Activity created on provider",
    Event.ActivityCreateFailed: "Failed to create activity",
    Event.TaskStarted: "Task started",
    Event.ScriptSent: "Script sent to provider",
    Event.CommandExecuted: "Script command executed",
    Event.GettingResults: "Getting script results",
    Event.ScriptFinished: "Script finished",
    Event.TaskAccepted: "Task accepted",  # by who?
    Event.TaskRejected: "Task rejected",  # by who?
    Event.WorkerFinished: "Worker finished",
    Event.DownloadStarted: "Download started",
    Event.DownloadFinished: "Download finished",
}


if get_type_args:
    # Check that `event_type_to_string` includes all event types from `Event`
    event_membertypes = set(value for value in vars(Event).values() if isinstance(value, type))
    assert event_membertypes.issubset(event_type_to_string.keys()), event_membertypes.difference(
        event_type_to_string.keys()
    )
