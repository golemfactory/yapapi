from functools import wraps

from ya_payment import ApiException as PaymentApiException
from ya_market import ApiException as MarketApiException
from ya_activity import ApiException as ActivityApiException
from ya_net import ApiException as NetApiException

from .exceptions import ObjectNotFound


all_api_exceptions = (PaymentApiException, MarketApiException, ActivityApiException, NetApiException)


def api_call_wrapper(f):
    @wraps(f)
    async def wrapper(self_or_cls, *args, **kwargs):
        try:
            return await f(self_or_cls, *args, **kwargs)
        except all_api_exceptions as e:
            if e.status == 404:
                raise ObjectNotFound(type(self_or_cls).__name__, self_or_cls._id)
            elif e.status == 410:
                #   DELETE on something that was already deleted
                #   (on some of the objects only)
                #   TODO: Is silencing OK? Log this maybe?
                pass
            else:
                raise
    return wrapper
