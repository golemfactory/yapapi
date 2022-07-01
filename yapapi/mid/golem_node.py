import asyncio

from yapapi import rest
from yapapi.engine import DEFAULT_DRIVER, DEFAULT_NETWORK, DEFAULT_SUBNET

from .payment import Allocation
from .market import Demand, Offer, Agreement


class GolemNode:
    def __init__(self):
        self._api_config = rest.Configuration()
        self.payment_driver = DEFAULT_DRIVER
        self.payment_network = DEFAULT_NETWORK
        self.subnet = DEFAULT_SUBNET

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, *exc_info):
        await self.close()

    async def start(self):
        #   TODO: in yapapi.Engine all APIs are wrapped in an AsyncExitStack,
        #         we probably should have it here as well
        self._ya_market_api = self._api_config.market()
        self._ya_activity_api = self._api_config.activity()
        self._ya_payment_api = self._api_config.payment()
        self._ya_net_api = self._api_config.net()

    async def close(self):
        await asyncio.gather(
            self._ya_market_api.close(),
            self._ya_activity_api.close(),
            self._ya_payment_api.close(),
            self._ya_net_api.close(),
        )

    def allocation(self, allocation_id) -> Allocation:
        return Allocation(self, allocation_id)

    def demand(self, demand_id) -> Demand:
        return Demand(self, demand_id)

    def offer(self, offer_id) -> Offer:
        return Offer(self, offer_id)

    def agreement(self, agreement_id) -> Agreement:
        return Agreement(self, agreement_id)

    def __str__(self):
        lines = [
            f"{type(self).__name__}(",
            f"  app_key = {self._api_config.app_key},",
            f"  subnet = {self.subnet},",
            f"  payment_driver = {self.payment_driver},",
            f"  payment_network = {self.payment_network},",
            f"  market_url = {self._api_config.market_url},",
            f"  payment_url = {self._api_config.payment_url},",
            f"  activity_url = {self._api_config.activity_url},",
            f"  net_url = {self._api_config.net_url},",
            f"  gsb_url = TODO? this is used by yagna only, but is part of the config?",
            f")",
        ]
        return "\n".join(lines)
