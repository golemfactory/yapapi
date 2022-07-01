from abc import ABC

from ya_market import RequestorApi, models as ya_models

from .golem_object import GolemObject
from .exceptions import ObjectNotFound


class PaymentApiObject(GolemObject, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_market_api)

    @property
    def model(self):
        my_name = type(self).__name__
        return ya_models.getattr(my_name)


class Demand(PaymentApiObject):
    async def _load_no_wrap(self):
        #   NOTE: this method is required because there is no get_demand(id)
        #         in ya_market
        all_demands = await self.api.get_demands()
        try:
            this_demands = [d for d in all_demands if d.demand_id == self.id]
            self._data = this_demands[0]
        except IndexError:
            raise ObjectNotFound('Demand', self.id)


class Offer(PaymentApiObject):
    async def _load_no_wrap(self, demand_id):
        self._data = await self.api.get_proposal_offer(demand_id, self.id)


class Agreement(PaymentApiObject):
    pass
