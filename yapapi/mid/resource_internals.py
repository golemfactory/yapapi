"""The only purpose of this file is to increase code readability by separating "deep" internals
from the "important" part od the app logic"""

from typing import get_args, no_type_check, Type, TypeVar, TYPE_CHECKING, Union

from ya_payment import models as payment_models, RequestorApi as PaymentApi
from ya_market import models as market_models, RequestorApi as MarketApi

if TYPE_CHECKING:
    from yapapi.mid.golem_node import GolemNode
    from yapapi.mid.resource import Resource
    from yapapi.mid import market


#########################
#   TYPING BLACK MAGIC
class _NULL:
    """Set this as a type to tell the typechecker that call is just invalid.

    This might be ugly, but keeps Resource inheritance tree simple."""


ResourceType = TypeVar("ResourceType", bound="Resource")
RequestorApiType = TypeVar("RequestorApiType", PaymentApi, MarketApi)
ModelType = TypeVar(
    "ModelType",
    payment_models.Allocation,
    market_models.Demand,
    market_models.Proposal,
    market_models.Agreement,
)
ParentType = TypeVar(
    "ParentType",
    _NULL,
    "market.Proposal",
    Union["market.Demand", "market.Proposal"]
)
ChildType = TypeVar(
    "ChildType",
    _NULL,
    "market.Proposal",
    Union["market.Proposal", "market.Agreement"],
)
EventType = TypeVar(
    "EventType",
    _NULL,
    Union[market_models.ProposalEvent, market_models.ProposalRejectedEvent],
)


@no_type_check
def get_requestor_api(cls: Type["Resource"], node: "GolemNode") -> RequestorApiType:
    """Return RequestorApi for a given cls, using class typing.

    This is very ugly, but should work well and simplifies the Resource inheritance.
    If we ever decide this is too ugly, it shouldn"t be hard to get rid of this.

    NOTE: this references only "internal" typing, so is invisible from the interface POV.
    """
    api_type = get_args(cls.__orig_bases__[0])[0]
    if api_type is PaymentApi:
        api = PaymentApi(node._ya_payment_api)
        assert type(api) is PaymentApi
        return api
    elif api_type is MarketApi:
        return MarketApi(node._ya_market_api)
    raise TypeError("This should never happen")
