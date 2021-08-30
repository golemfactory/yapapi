from dataclasses import dataclass
from ipaddress import ip_address, ip_network, IPv4Address, IPv6Address, IPv4Network, IPv6Network
from typing import Optional, Union, Dict
from uuid import uuid4

from ya_net import (
    ApiClient,
    RequestorApi,
    models as yan,
)


IpAddress = Union[IPv4Address, IPv6Address]
IpNetwork = Union[IPv4Network, IPv6Network]


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
        network = Network(self._api, ip, owner_id, network_id, mask, gateway)
        await network.create()
        return network


@dataclass
class Node:
    network: "Network"
    node_id: str
    ip: str

    def get_deploy_args(self) -> Dict:
        deploy_args = {
            "net": [
                {
                    "id": self.network.network_id,
                    "ip": str(self.network.network_address),
                    "mask": str(self.network.netmask),
                    "nodeIp": str(self.ip),
                    "nodes": self.network.nodes_dict,
                }
            ]
        }
        return deploy_args


class Network:
    def __init__(
        self,
        api: RequestorApi,
        ip: str,
        owner_id: str,
        owner_ip: Optional[str] = None,
        network_id: Optional[str] = None,
        mask: Optional[str] = None,
        gateway: Optional[str] = None,
    ):
        self._api = api
        network_ip = f"{ip}/{mask}" if mask else ip
        try:
            self._ip_network: IpNetwork = ip_network(network_ip, strict=False)
        except ValueError as e:
            raise NetworkError(f"{e}.")
        self._hosts = self._ip_network.hosts()

        self._network_id = network_id or "0x" + uuid4().hex
        self._gateway = gateway
        self._owner_id = owner_id
        self._owner_ip = owner_ip or self._next_address()
        self._nodes: Dict[str, Node] = dict()

    @property
    def owner_ip(self) -> str:
        return str(self._owner_ip)

    @property
    def network_address(self) -> str:
        return str(self._ip_network.network_address)

    @property
    def netmask(self) -> str:
        return str(self._ip_network.netmask)

    @property
    def gateway(self) -> Optional[str]:
        if self._gateway:
            return str(self._gateway)
        return None

    @property
    def nodes_dict(self) -> Dict[str, str]:
        return {str(v.ip): k for k, v in self._nodes.items()}

    @property
    def network_id(self):
        return self._network_id

    async def create(self):
        await self._api.create_network(
            yan.Network(
                self.network_id, self.network_address, mask=self.netmask, gateway=self.gateway
            )
        )
        await self.add_address(self.owner_ip)

    def _ensure_ip_in_network(self, ip: str):
        if ip_address(ip) not in self._ip_network:
            raise NetworkError(
                f"The given IP ('{ip}') address must belong to the network ('{self._ip_network.with_netmask}')."
            )

    async def add_address(self, ip: str):
        """Assign the given IP address to the requestor in the Network."""
        self._ensure_ip_in_network(ip)

        if ip in self.nodes_dict.keys():
            raise NetworkError(f"'{ip}' has already been assigned in this network.")

        address = yan.Address(ip)
        self._nodes[self._owner_id] = Node(network=self, node_id=self._owner_id, ip=ip)
        await self._api.add_address(self._network_id, address)

    async def add_node(self, node_id: str, ip: Optional[str] = None) -> Node:
        """Add a new node to the network."""
        if ip:
            self._ensure_ip_in_network(ip)
            if ip in self.nodes_dict.keys():
                raise NetworkError(f"'{ip}' has already been assigned in this network.")
        else:
            while True:
                ip = str(self._next_address())
                if ip not in self.nodes_dict.keys():
                    break

        await self._api.add_node(self._network_id, yan.Node(node_id, ip))
        node = Node(network=self, node_id=node_id, ip=ip)
        self._nodes[node_id] = node
        return node

    def _next_address(self) -> IpAddress:
        try:
            return next(self._hosts)  # type: ignore
        except StopIteration:
            raise NetworkError(f"No more addresses available in '{self._ip_network.with_netmask}'.")

    def __str__(self) -> str:
        return f"""Network {{
    id: {self._network_id}
    ip: {self.network_address}
    mask: {self.netmask}
    owner_ip: {self._owner_ip}
    nodes: {self.nodes_dict}
}}"""


class NetworkError(Exception):
    pass
