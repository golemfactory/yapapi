from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Callable, List, Optional, TYPE_CHECKING

from .api_call_wrapper import api_call_wrapper
from .yagna_event_collector import YagnaEventCollector


if TYPE_CHECKING:
    from .golem_node import GolemNode


class CachedSingletonId(ABCMeta):
    def __call__(cls, node: "GolemNode", id_: str, *args, **kwargs):  # type: ignore
        assert isinstance(cls, type(Resource))  # mypy
        if args:
            #   Sanity check: when data is passed, it must be a new resource
            #   (TODO: maybe we should only check if we got the same data?)
            assert id_ not in node._resources[cls]

        if id_ not in node._resources[cls]:
            obj = super(CachedSingletonId, cls).__call__(node, id_, *args, **kwargs)  # type: ignore
            node._resources[cls][id_] = obj
        return node._resources[cls][id_]


class Resource(ABC, metaclass=CachedSingletonId):
    def __init__(self, node: "GolemNode", id_: str, data: Any = None):
        self._node = node
        self._id = id_
        self._data = data

        self._event_collector: Optional[YagnaEventCollector] = None

    @property
    @abstractmethod
    def api(self):
        pass

    async def get_data(self, force=False) -> Any:
        if self._data is None or force:
            self._data = await self._get_data()
        return self._data

    @api_call_wrapper()
    async def _get_data(self) -> Any:
        #   NOTE: this method is often overwritten in subclasses
        #   TODO: typing? self._data typing?
        get_method = getattr(self.api, self._get_method_name)
        return await get_method(self._id)

    @classmethod
    @api_call_wrapper()
    async def get_all(cls, node: "GolemNode") -> List["Resource"]:
        api = cls._get_api(node)
        get_all_method = getattr(api, cls._get_all_method_name())
        data = await get_all_method()

        resources = []
        id_field = f'{cls.__name__.lower()}_id'
        for raw in data:
            id_ = getattr(raw, id_field)
            resources.append(cls(node, id_, raw))
        return resources

    @property
    def id(self) -> str:
        return self._id

    @property
    def data(self) -> Any:
        if self._data is None:
            raise RuntimeError(f"Unknown {type(self).__name__} data - call get_data() first")
        return self._data

    @property
    def node(self) -> "GolemNode":
        return self._node

    def start_collecting_events(self, get_events: Callable, *args, **kwargs) -> None:
        assert self._event_collector is None
        self._event_collector = YagnaEventCollector(get_events, list(args), kwargs)
        self._event_collector.start()

    async def stop_collecting_events(self) -> None:
        if self._event_collector is not None:
            await self._event_collector.stop()

    @property
    def _get_method_name(self) -> str:
        return f'get_{type(self).__name__.lower()}'

    @classmethod
    def _get_all_method_name(cls) -> str:
        return f'get_{cls.__name__.lower()}s'

    @classmethod
    def _get_api(cls, node: "GolemNode"):
        return cls(node, '').api

    def __str__(self):
        return f'{type(self).__name__}({self._id})'
