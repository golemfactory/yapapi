from abc import ABC
from typing import TYPE_CHECKING

from ya_payment import ApiException as PaymentApiException

from .exceptions import ObjectNotFound


if TYPE_CHECKING:
    from .golem_node import GolemNode


class GolemObject(ABC):
    def __init__(self, node: "GolemNode", id_: str):
        self._node = node
        self._id = id_
        self._data = None

    async def load(self):
        try:
            await self._load_no_wrap()
        except PaymentApiException as e:
            if e.status == 404:
                raise ObjectNotFound(type(self).__name__, self._id)
            else:
                raise

    async def _load_no_wrap(self):
        get_method = getattr(self.api, self._get_method_name)
        self._data = await get_method(self._id)

    @property
    def id(self):
        return self._id

    @property
    def data(self):
        return self._data

    @property
    def _get_method_name(self):
        return f'get_{type(self).__name__.lower()}'

    def __str__(self):
        return f'{type(self).__name__}({self._id})'
