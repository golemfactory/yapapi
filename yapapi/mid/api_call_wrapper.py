from functools import wraps
from typing import Awaitable, Callable, List, TypeVar, Optional
from typing_extensions import ParamSpec

from ya_payment import ApiException as PaymentApiException
from ya_market import ApiException as MarketApiException
from ya_activity import ApiException as ActivityApiException
from ya_net import ApiException as NetApiException

from .exceptions import ResourceNotFound


all_api_exceptions = (PaymentApiException, MarketApiException, ActivityApiException, NetApiException)
P = ParamSpec("P")
R = TypeVar("R")


def api_call_wrapper(
    ignore: List[int] = []
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    def outer_wrapper(f: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(f)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
            try:
                return await f(*args, **kwargs)
            except all_api_exceptions as e:
                if e.status in ignore:
                    return None
                elif e.status == 404:
                    self = args[0]
                    raise ResourceNotFound(type(self).__name__, self.id)  # type: ignore  # 404 -> we have id
                else:
                    raise
        return wrapper  # type: ignore  # I don't understand this :/
    return outer_wrapper
