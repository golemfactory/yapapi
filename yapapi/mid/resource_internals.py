"""The only purpose of this file is to increase code readability by separating "deep" internals
from the "important" part od the app logic"""

from abc import ABCMeta
from typing import TypeVar, TYPE_CHECKING

from yapapi.mid.events import NewResource

from ya_payment import models as payment_models, RequestorApi as PaymentApi
from ya_market import models as market_models, RequestorApi as MarketApi

if TYPE_CHECKING:
    from .golem_node import GolemNode
    from .resource import Resource


#########################
#   TYPING BLACK MAGIC
RequestorApiType = TypeVar('RequestorApiType', PaymentApi, MarketApi)
PaymentResourceType = payment_models.Allocation
MarketResourceType = TypeVar(
    'MarketResourceType',
    market_models.Demand,
    market_models.Proposal,
    market_models.Agreement,
)
ResourceType = TypeVar(
    'ResourceType',
    payment_models.Allocation,
    market_models.Demand,
    market_models.Proposal,
    market_models.Agreement,
)


class ResourceMeta(ABCMeta):
    """Resources metaclass. Ensures a single instance per resource id. Emits the NewResource event."""

    def __call__(cls, node: "GolemNode", id_: str, *args, **kwargs):  # type: ignore
        assert isinstance(cls, type(Resource))  # mypy
        if args:
            #   Sanity check: when data is passed, it must be a new resource
            assert id_ not in node._resources[cls]

        if id_ not in node._resources[cls]:
            obj = super(ResourceMeta, cls).__call__(node, id_, *args, **kwargs)  # type: ignore
            node._resources[cls][id_] = obj
            node.event_bus.emit(NewResource(obj))
        return node._resources[cls][id_]
