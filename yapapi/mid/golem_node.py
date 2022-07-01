from yapapi import rest
from yapapi.engine import DEFAULT_DRIVER, DEFAULT_NETWORK, DEFAULT_SUBNET


class GolemNode:
    def __init__(self):
        self._api_config = rest.Configuration()
        self.payment_driver = DEFAULT_DRIVER
        self.payment_network = DEFAULT_NETWORK
        self.subnet = DEFAULT_SUBNET

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
