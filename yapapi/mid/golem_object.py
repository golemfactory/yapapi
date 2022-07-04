from abc import ABC
import asyncio
from collections import defaultdict
from functools import wraps
from typing import Any, Awaitable, DefaultDict, List, Optional, TYPE_CHECKING

from ya_payment import ApiException as PaymentApiException
from ya_market import ApiException as MarketApiException
from ya_activity import ApiException as ActivityApiException
from ya_net import ApiException as NetApiException

from .exceptions import ObjectNotFound


if TYPE_CHECKING:
    from .golem_node import GolemNode


all_api_exceptions = (PaymentApiException, MarketApiException, ActivityApiException, NetApiException)


def capture_api_exception(f):
    @wraps(f)
    async def wrapper(self_or_cls, *args, **kwargs):
        try:
            return await f(self_or_cls, *args, **kwargs)
        except all_api_exceptions as e:
            if e.status == 404:
                assert isinstance(self_or_cls, GolemObject)
                raise ObjectNotFound(type(self_or_cls).__name__, self_or_cls._id)
            elif e.status == 410:
                #   DELETE on something that was already deleted
                #   (on some of the objects only)
                #   TODO: Is silencing OK? Log this maybe?
                pass
            else:
                raise
    return wrapper


class GolemObject(ABC):
    def __init__(self, node: "GolemNode", id_: str, data=None):
        self._node = node
        self._id = id_
        self._data = data

        self._collect_events_task: Optional[asyncio.Task] = None
        self._events: DefaultDict[type, List[Any]] = defaultdict(list)

    @capture_api_exception
    async def load(self, *args, **kwargs) -> None:
        await self._load_no_wrap(*args, **kwargs)

    @capture_api_exception
    async def delete(self) -> None:
        await self._delete_no_wrap()

    async def _load_no_wrap(self) -> None:
        get_method = getattr(self.api, self._get_method_name)
        self._data = await get_method(self._id)

    async def _delete_no_wrap(self) -> None:
        delete_method = getattr(self.api, self._delete_method_name)
        await delete_method(self._id)

    @classmethod
    @capture_api_exception
    async def get_all(cls, node: "GolemNode"):
        api = cls._get_api(node)
        get_all_method = getattr(api, cls._get_all_method_name())
        data = await get_all_method()

        objects = []
        id_field = f'{cls.__name__.lower()}_id'
        for raw in data:
            id_ = getattr(raw, id_field)
            objects.append(cls(node, id_, raw))
        return tuple(objects)

    @property
    def id(self):
        return self._id

    @property
    def data(self):
        return self._data

    @property
    def node(self):
        return self._node

    def start_collecting_events(self, get_events: Awaitable, *args, **kwargs) -> None:
        assert self._collect_events_task is None
        coroutine = self._get_events(get_events, *args, **kwargs)
        self._collect_events_task = asyncio.create_task(coroutine)

    async def _get_events(self, get_events: Awaitable, *args, **kwargs) -> None:
        while True:
            events = await get_events(*args, **kwargs)
            for event in events:
                self._events[type(event)].append(event)

            if not events:
                await asyncio.sleep(1)

    def stop_collecting_events(self) -> None:
        if not self._collect_events_task:
            return
        self._collect_events_task.cancel()
        self._collect_events_task = None

    @property
    def _get_method_name(self):
        return f'get_{type(self).__name__.lower()}'

    @classmethod
    def _get_all_method_name(cls):
        return f'get_{cls.__name__.lower()}s'

    @classmethod
    def _get_api(cls, node: "GolemNode"):
        return cls(node, '').api

    def __str__(self):
        return f'{type(self).__name__}({self._id})'
