from functools import wraps
from typing import Callable, List

from ya_payment import ApiException as PaymentApiException
from ya_market import ApiException as MarketApiException
from ya_activity import ApiException as ActivityApiException
from ya_net import ApiException as NetApiException

from .exceptions import ResourceNotFound


all_api_exceptions = (PaymentApiException, MarketApiException, ActivityApiException, NetApiException)


def api_call_wrapper(ignore: List[int] = []) -> Callable[[Callable], Callable]:
    def outer_wrapper(f) -> Callable:
        @wraps(f)
        async def wrapper(*args, **kwargs):
            try:
                return await f(*args, **kwargs)
            except all_api_exceptions as e:
                if e.status in ignore:
                    pass
                elif e.status == 404:
                    self = args[0]
                    raise ResourceNotFound(type(self).__name__, self.id)
                else:
                    raise
        return wrapper
    return outer_wrapper
