from typing import Optional

from ya_net import (
    ApiClient,
    RequestorApi,
    models as yan,
)

from yapapi.net import Network


class Net(object):
    """Mid-level interface to the Net REST API."""

    def __init__(self, api_client: ApiClient):
        self._api = RequestorApi(api_client)

    async def create_network(
        self,
        owner_id: str,
        ip: str,
        network_id: Optional[str] = None,
        mask: Optional[str] = None,
        gateway: Optional[str] = None,
    ) -> "Network":
        network = Network(self, ip, owner_id, network_id, mask, gateway)
        # create the network in yagna
        await self._api.create_network(
            yan.Network(
                network.network_id, network.network_address, mask=network.netmask, gateway=network.gateway
            )
        )

        # add requestor's own address to the network
        await network.add_address(network.owner_ip)
        return network

    async def add_address(self, network: Network, ip: str):
        address = yan.Address(ip)
        await self._api.add_address(network.network_id, address)

    async def add_node(self, network, node_id: str, ip: str):
        await self._api.add_node(network.network_id, yan.Node(node_id, ip))

