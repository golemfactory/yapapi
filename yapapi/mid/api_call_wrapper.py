from functools import wraps
from typing import List

from ya_payment import ApiException as PaymentApiException
from ya_market import ApiException as MarketApiException
from ya_activity import ApiException as ActivityApiException
from ya_net import ApiException as NetApiException

from .exceptions import ResourceNotFound


all_api_exceptions = (PaymentApiException, MarketApiException, ActivityApiException, NetApiException)


def api_call_wrapper(ignored_errors: List[int] = []):
    def outer_wrapper(f):
        @wraps(f)
        async def wrapper(self_or_cls, *args, **kwargs):
            try:
                return await f(self_or_cls, *args, **kwargs)
            except all_api_exceptions as e:
                if e.status in ignored_errors:
                    pass
                elif e.status == 404:
                    raise ResourceNotFound(type(self_or_cls).__name__, self_or_cls._id)
                else:
                    raise
        return wrapper
    return outer_wrapper
