from typing import Optional, TYPE_CHECKING

import ya_payment.models as yap
from ya_payment import RequestorApi, ApiException

from .exceptions import ObjectNotFound


if TYPE_CHECKING:
    from .golem_node import GolemNode


class Allocation:
    def __init__(self, node: "GolemNode", id_: str):
        self._node = node
        self._id = id_
        self._data: Optional[yap.Allocation] = None
    
    @property
    def data(self):
        return self._data

    async def load(self):
        api = RequestorApi(self._node._ya_payment_api)
        try:
            self._data = await api.get_allocation(self._id)
        except ApiException as e:
            if e.status == 404:
                raise ObjectNotFound(type(self).__name__, self._id)
            else:
                raise

    def __str__(self):
        return f'{type(self).__name__}({self._id})'
