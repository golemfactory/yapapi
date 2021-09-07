from typing import Optional

from ya_net import (
    ApiClient,
    RequestorApi,
    models as yan,
)


class Net(object):
    """Mid-level interface to the Net REST API."""

    def __init__(self, api_client: ApiClient):
        self._api = RequestorApi(api_client)

    async def create_network(
        self, network_address: str, netmask: Optional[str], gateway: Optional[str]
    ) -> str:
        yan_network = await self._api.create_network(
            yan.Network(
                network_address,
                mask=netmask,
                gateway=gateway,
            )
        )
        return yan_network.id

    async def add_address(self, network_id: str, ip: str):
        address = yan.Address(ip)
        await self._api.add_address(network_id, address)

    async def add_node(self, network_id: str, node_id: str, ip: str):
        await self._api.add_node(network_id, yan.Node(node_id, ip))
