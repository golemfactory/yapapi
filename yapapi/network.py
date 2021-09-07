import asyncio
from dataclasses import dataclass
from ipaddress import ip_address, ip_network, IPv4Address, IPv6Address, IPv4Network, IPv6Network
from typing import Dict, Optional, Union, TYPE_CHECKING

IpAddress = Union[IPv4Address, IPv6Address]
IpNetwork = Union[IPv4Network, IPv6Network]


if TYPE_CHECKING:
    from yapapi.rest.net import Net


@dataclass
class Node:
    """
    Describes a node in a VPN, mapping a Golem node id to an IP address.
    """

    network: "Network"
    """The Network (the specific VPN) this node is part of."""

    node_id: str
    """Golem id of the node."""

    ip: str
    """IP address of this node in this particular VPN."""

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
    """
    Describes a VPN created between the requestor and the provider nodes within Golem Network.
    """

    @classmethod
    async def create(
        cls,
        net_api: "Net",
        ip: str,
        owner_id: str,
        owner_ip: Optional[str] = None,
        mask: Optional[str] = None,
        gateway: Optional[str] = None,
    ):
        """Create a new VPN.

        :param net_api: the mid-level binding used directly to perform calls to the REST API.
        :param ip: the IP address of the network. May contain netmask, e.g. "192.168.0.0/24"
        :param owner_id: the node ID of the owner of this VPN (the requestor)
        :param owner_ip: the desired IP address of the requestor node within the newly-created Network
        :param mask: Optional netmask (only if not provided within the `ip` argument)
        :param gateway: Optional gateway address for the network

        :return: a Network object allowing further manipulation of the created VPN
        """

        network = cls(net_api, ip, owner_id, owner_ip, mask, gateway)

        # create the network in yagna and set the id
        network._network_id = await net_api.create_network(network)

        # add requestor's own address to the network
        await network.add_address(network.owner_ip)

        return network

    def __init__(
        self,
        net_api: "Net",
        ip: str,
        owner_id: str,
        owner_ip: Optional[str] = None,
        mask: Optional[str] = None,
        gateway: Optional[str] = None,
    ):
        """
        :param net_api: the mid-level binding used directly to perform calls to the REST API.
        :param ip: the IP address of the network. May contain netmask, e.g. "192.168.0.0/24"
        :param owner_id: the node ID of the owner of this VPN (the requestor)
        :param owner_ip: the desired IP address of the requestor node within the newly-created Network
        :param mask: Optional netmask (only if not provided within the `ip` argument)
        :param gateway: Optional gateway address for the network
        """

        self._net_api = net_api
        network_ip = f"{ip}/{mask}" if mask else ip
        try:
            self._ip_network: IpNetwork = ip_network(network_ip, strict=False)
        except ValueError as e:
            raise NetworkError(f"{e}.")
        self._hosts = self._ip_network.hosts()

        self._network_id: Optional[str] = None
        self._gateway = gateway
        self._owner_id = owner_id
        self._owner_ip = owner_ip or self._next_address()

        self._nodes: Dict[str, Node] = dict()
        """the mapping between a Golem node id and a Node in this VPN."""

        self._nodes_lock = asyncio.Lock()
        """an asyncio lock used to synchronize access to the `_nodes` mapping."""

    def __str__(self) -> str:
        return f"""Network {{
        id: {self._network_id}
        ip: {self.network_address}
        mask: {self.netmask}
        owner_ip: {self._owner_ip}
        nodes: {self.nodes_dict}
    }}"""

    @property
    def owner_ip(self) -> str:
        """the IP address of the requestor node within the Network"""
        return str(self._owner_ip)

    @property
    def network_address(self) -> str:
        """The network address of this network, without a netmask."""
        return str(self._ip_network.network_address)

    @property
    def netmask(self) -> str:
        """The netmask of this network."""
        return str(self._ip_network.netmask)

    @property
    def gateway(self) -> Optional[str]:
        """The gateway address within this network, if provided."""
        if self._gateway:
            return str(self._gateway)
        return None

    @property
    def nodes_dict(self) -> Dict[str, str]:
        """Mapping between the IP addresses and Node IDs of the nodes within this network."""
        return {str(v.ip): k for k, v in self._nodes.items()}

    @property
    def network_id(self) -> Optional[str]:
        """The automatically-generated, unique ID of this VPN."""
        return self._network_id

    def _ensure_ip_in_network(self, ip: str):
        """Ensure the given IP address belongs to the network address range within this VPN."""
        if ip_address(ip) not in self._ip_network:
            raise NetworkError(
                f"The given IP ('{ip}') address must belong to the network ('{self._ip_network.with_netmask}')."
            )

    async def add_address(self, ip: str):
        """Assign the given IP address to the requestor in the Network.

        :param ip: the IP address to assign to the requestor node.
        """
        self._ensure_ip_in_network(ip)

        async with self._nodes_lock:
            if ip in self.nodes_dict.keys():
                raise NetworkError(f"'{ip}' has already been assigned in this network.")

            self._nodes[self._owner_id] = Node(network=self, node_id=self._owner_id, ip=ip)

        await self._net_api.add_address(self, ip)

    async def add_node(self, node_id: str, ip: Optional[str] = None) -> Node:
        """Add a new node to the network.

        :param node_id: Node ID within the Golem network of this VPN node.
        :param ip: IP address to assign to this node.
        """
        async with self._nodes_lock:
            if ip:
                self._ensure_ip_in_network(ip)
                if ip in self.nodes_dict.keys():
                    raise NetworkError(f"'{ip}' has already been assigned in this network.")
            else:
                while True:
                    ip = str(self._next_address())
                    if ip not in self.nodes_dict.keys():
                        break

            node = Node(network=self, node_id=node_id, ip=ip)
            self._nodes[node_id] = node

        await self._net_api.add_node(self, node_id, ip)

        return node

    def _next_address(self) -> IpAddress:
        """Provide the next available IP address within this Network.

        :raises NetworkError in case the network ran out of available addresses.
        """
        try:
            return next(self._hosts)  # type: ignore
        except StopIteration:
            raise NetworkError(f"No more addresses available in '{self._ip_network.with_netmask}'.")


class NetworkError(Exception):
    pass
