from abc import ABC
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from yapapi.mid.resource import Resource


class Event(ABC):
    pass


class ResourceEvent(Event, ABC):
    def __init__(self, resource: "Resource"):
        self.resource = resource

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.resource})'


class NewResource(ResourceEvent):
    """Emitted when a new :class:`Resource` object is created.

    There are three distinct scenarios possible:
    A) We create a new resource, e.g. with `GolemNode.create_allocation()`
    B) We start interacting with some resource that was created by us before,
       but not with this particular GolemNode instance, eg. GolemNode.allocation(allocation_id)
    C) We find a resource created by someone else (e.g. a Proposal)

    There's no difference between these scenarions from the POV of this event.
    """


class ResourceDataChanged(ResourceEvent):
    """Emitted when `data` attribute of :class:`Resource` changes.

    Note that this event is **not** emitted when the data "would have changed if we
    requested new data, but we didn't request it".

    Change might be caused by us or by some party.
    Second argument is the old data (before the change), so comparing
    event.resource.data with event.old_data shows what changed.

    NULL (i.e. empty) change is not a change, even if we explicitly sent a resource-changing call.
    """

    def __init__(self, resource: "Resource", old_data: Any):
        super().__init__(resource)
        self.old_data = old_data

        #   TODO: this should be included in typing, but I don't know how to do this
        assert old_data is None or type(resource.data) is type(old_data)


class ResourceChangePossible(ResourceEvent):
    """Emitted when we receive an information that the resource.data might be outdated.

    E.g. this is emitted for a `Proposal` when an agreement is created based on a proposal.

    It is **not** guaranteed that anything really changed, but e.g. if you want to keep some :class:`Resource`
    objects as up-to-date as possible, you might consider executing `await event.resorce.get_data(force=True)` on
    every `ResourceChangePossible` event that references resources you are interested in.

    NOTE: This is **NOT IMPLEMENTED** now, as it's pretty complex and useless now, and maybe not a good idea at all.
    """


class ResourceClosed(ResourceEvent):
    """Emitted when a resource is deleted or rendered unusable.

    Usual case is when we delete a resource (e.g. `await allocation.release()`),
    or when the lifespan of a resource ends (e.g. `await agreement.terminate()`),
    but this can be also emitted when we notice that resource was deleted by someone
    else (TODO: when? is this possible at all? e.g. expired proposal?).

    This is emitted only when something changes - when we create a new Resource object
    for some closed resource (e.g. `GolemNode.agreement(id_of_a_past_agreement)`), this
    will not be emitted.

    This event should never be emitted more than once for a given :class:`Resource`.
    """
