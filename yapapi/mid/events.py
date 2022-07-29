from abc import ABC
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from yapapi.mid.resource import Resource


class Event(ABC):
    """Base class for all events."""
    pass


class ResourceEvent(Event, ABC):
    """Base class for all events related to a particular :class:`~yapapmi.mid.resource.Resource`."""
    def __init__(self, resource: "Resource"):
        self._resource = resource

    @property
    def resource(self) -> "Resource":
        """Resource related to this :class:`ResourceEvent`."""
        return self._resource

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.resource})'


class NewResource(ResourceEvent):
    """Emitted when a new :class:`Resource` object is created.

    There are three distinct scenarios possible:

    * We create a new resource, e.g. with :any:`GolemNode.create_allocation()`
    * We start interacting with some resource that was created by us before,
      but not with this particular GolemNode instance, eg. :any:`GolemNode.allocation()`
    * We find a resource created by someone else (e.g. a :any:`Proposal`)

    There's no difference between these scenarions from the POV of this event.
    """


class ResourceDataChanged(ResourceEvent):
    """Emitted when `data` attribute of a :class:`Resource` changes.

    This event is **not** emitted when the data "would have changed if we
    requested new data, but we didn't request it". In other words, we don't listen for
    `yagna`-side changes, only react to changes already noticed in the context of a :any:`GolemNode`.

    Second argument is the old data (before the change), so comparing
    `event.resource.data` with `event.old_data` shows what changed.

    NULL (i.e. empty) change doesn't trigger the change, even if we explicitly sent a resource-changing call.
    """

    def __init__(self, resource: "Resource", old_data: Any):
        super().__init__(resource)
        self._old_data = old_data

        #   TODO: this should be included in typing, but I don't know how to do this
        assert old_data is None or type(resource.data) is type(old_data)

    @property
    def old_data(self) -> Any:
        """Value of `self.resource.data` before the change."""
        return self._old_data


class ResourceChangePossible(ResourceEvent):
    """Emitted when we receive an information that a :any:`Resource.data` might be outdated.

    E.g. this is emitted for a `Proposal` when an agreement is created based on this proposal.

    It is **not** guaranteed that anything really changed, but e.g. if you want to keep some :class:`Resource`
    objects as up-to-date as possible, you might consider something like::

        async def update_resource(event: ResourceEvent) -> None:
            await event.resource.get_data(force=True)

        golem.event_bus.resource_listen(
            update_resource,
            event_classes=[ResourceChangePossible],
            ids=[an_important_resource.id]
        )

    NOTE: This is **NOT IMPLEMENTED** yet, as it's pretty complex and useless now, and maybe not a good idea at all.
    """


class ResourceClosed(ResourceEvent):
    """Emitted when a resource is deleted or rendered unusable.

    Usual case is when we delete a resource (e.g. :any:`Allocation.release()`),
    or when the lifespan of a resource ends (e.g. :any:`Agreement.terminate()`),
    but this can be also emitted when we notice that resource was deleted by someone
    else (TODO: when? is this possible at all? e.g. expired proposal?).

    This is emitted only when something changes. Creating a new Resource object
    for an already closed resource (e.g. by passing an id of a terminated agreement to
    :any:`GolemNode.agreement`) does not trigger this event.

    This event should never be emitted more than once for a given :class:`Resource`.
    """
