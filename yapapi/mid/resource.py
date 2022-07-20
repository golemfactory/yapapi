import asyncio
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, AsyncIterator, List, Optional, TYPE_CHECKING

from .api_call_wrapper import api_call_wrapper


if TYPE_CHECKING:
    from .golem_node import GolemNode


class CachedSingletonId(ABCMeta):
    def __call__(cls, node: "GolemNode", id_: str, *args, **kwargs):  # type: ignore
        assert isinstance(cls, type(Resource))  # mypy
        if args:
            #   Sanity check: when data is passed, it must be a new resource
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

        self._parent: Optional[Resource] = None
        self._children: List[Resource] = []

    ####################
    #   RESOURCE TREE
    @property
    def parent(self) -> "Resource":
        assert self._parent is not None
        return self._parent

    @parent.setter
    def parent(self, parent: "Resource") -> None:
        assert self._parent is None
        self._parent = parent

    def add_child(self, child: "Resource") -> None:
        child.parent = self
        self._children.append(child)

    def children(self) -> List["Resource"]:
        return self._children.copy()

    async def child_aiter(self) -> AsyncIterator["Resource"]:
        #   TODO: make this more efficient (remove sleep)
        #         (e.g. by setting some awaitable to ready in add_child)
        cnt = 0
        while True:
            if cnt < len(self._children):
                yield self._children[cnt]
                cnt += 1
            else:
                await asyncio.sleep(0.1)

    ####################
    #   PROPERTIES
    @property
    @abstractmethod
    def api(self):
        pass

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

    ####################
    #   DATA LOADING
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

    ###################
    #   OTHER
    async def stop_collecting_events(self) -> None:
        #   NOTE: this is ugly, but provides compatible interfaces.
        pass

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
