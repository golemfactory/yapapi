from dataclasses import dataclass
from ipaddress import (
    ip_network,
    IPv4Address,
    IPv6Address,
    IPv4Network,
    IPv6Network
)
from typing import Optional, Union, Dict
from uuid import uuid4

from ya_net import (
    ApiClient,
    RequestorApi,
    models,
)


IpAddress = Union[IPv4Address, IPv6Address]
IpNetwork = Union[IPv4Network, IPv6Network]


@dataclass
class Node(object):
    network_id: str
    node_id: str
    ip: str


class Swarm(object):
    def __init__(self, api: RequestorApi, node_id: str, network_id: str, ip: str):
        self._api = api
        self._node_id = node_id
        self._network_id = network_id
        self._network: IpNetwork = ip_network(ip, strict=False)
        self._hosts = self._network.hosts()
        self._address = self._next_address()
        self._nodes = dict()

    def deploy(self, node: Node) -> Dict:
        deploy = {
            "net": [{
                "id": node.network_id,
                "ip": str(self._network.network_address),
                "mask": str(self._network.netmask),
                "nodeIp": str(node.ip),
                "nodes": {str(v): k for k, v in self._nodes.items()},
            }]
        }

        print("Deploy:", deploy)
        return deploy

    @property
    def address(self) -> IpAddress:
        return self._address

    async def add_address(self, ip: IpAddress):
        address = models.Address(str(ip))
        await self._api.add_address(self._network_id, address)
        self._nodes[self._node_id] = ip

    async def add_node(self, node_id: str) -> Node:
        ip = self._next_address()
        node = models.Node(node_id, str(ip))
        await self._api.add_node(self._network_id, node)
        self._nodes[node_id] = ip
        return Node(network_id=self._network_id, node_id=node_id, ip=str(ip))

    def _next_address(self) -> IpAddress:
        return next(self._hosts)

    def __str__(self) -> str:
        return f"""Swarm {{
    id: {self._network_id}
    ip: {self._network.network_address}
    mask: {self._network.netmask}
    node: {self._address}
}}"""


class Network(object):
    """Mid-level interface to the Net REST API."""

    def __init__(self, api_client: ApiClient):
        self._api = RequestorApi(api_client)

    async def create(self, node_id: str, ip: str, mask: Optional[str] = None, gateway: Optional[str] = None) -> Swarm:
        network_id = '0x' + uuid4().hex
        network = models.Network(network_id, ip, mask=mask, gateway=gateway)
        await self._api.create_network(network)
        swarm = Swarm(self._api, node_id, network_id, ip)
        await swarm.add_address(swarm.address)
        print(str(swarm))
        return swarm


class SwarmBuilder(object):

    def __init__(self, ip: str, mask: Optional[str] = None, gateway: Optional[str] = None):
        self.ip = ip
        self.mask = mask
        self.gateway = gateway

    async def build(self, node_id: str, network: Network) -> Swarm:
        return await network.create(node_id=node_id, ip=self.ip, mask=self.mask, gateway=self.gateway)
