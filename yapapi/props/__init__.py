from .base import Model
from dataclasses import dataclass, field
from typing import Optional
from decimal import Decimal
from datetime import datetime


@dataclass
class Identification(Model):
    name: Optional[str] = field(default=None, metadata={"key": "golem.node.id.name"})
    subnet_tag: Optional[str] = field(default=None, metadata={"key": "golem.node.debug.subnet"})


IdentificationKeys = Identification.keys()


@dataclass()
class Activity(Model):
    """Activity-related Properties"""

    cost_cap: Optional[Decimal] = field(default=None, metadata={"key": "golem.activity.cost_cap"})
    """Sets a Hard cap on total cost of the Activity (regardless of the usage vector or
    pricing function). The Provider is entitled to 'kill' an Activity which exceeds the
    capped cost amount indicated by Requestor.
    """

    cost_warning: Optional[Decimal] = field(
        default=None, metadata={"key": "golem.activity.cost_warning"}
    )
    """Sets a Soft cap on total cost of the Activity (regardless of the usage vector or
    pricing function). When the cost_warning amount is reached for the Activity,
    the Provider is expected to send a Debit Note to the Requestor, indicating
    the current amount due
    """

    timeout_secs: Optional[float] = field(
        default=None, metadata={"key": "golem.activity.timeout_secs"}
    )
    """A timeout value for batch computation (eg. used for container-based batch
    processes). This property allows to set the timeout to be applied by the Provider
    when running a batch computation: the Requestor expects the Activity to take
    no longer than the specified timeout value - which implies that
    eg. the golem.usage.duration_sec counter shall not exceed the specified
    timeout value.
    """

    expiration: Optional[datetime] = field(
        default=None, metadata={"key": "golem.srv.comp.expiration"}
    )
    """
    """


ActivityKeys = Activity.keys()
