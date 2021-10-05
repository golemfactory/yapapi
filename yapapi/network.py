import asyncio
import logging
from dataclasses import dataclass
from ipaddress import ip_address, ip_network, IPv4Address, IPv6Address, IPv4Network, IPv6Network
from statemachine import State, StateMachine  # type: ignore
from typing import Dict, Optional, Union
from urllib.parse import urlparse

import yapapi
from ya_net.exceptions import ApiException

logger = logging.getLogger("yapapi.network")

IpAddress = Union[IPv4Address, IPv6Address]
IpNetwork = Union[IPv4Network, IPv6Network]


@dataclass
class Node:
    """
    Describes a node in a VPN, mapping a Golem node id to an IP address.
    """

    network: "Network"
    """The :class:`Network` (the specific VPN) this node is part of."""

    node_id: str
    """Golem id of the node."""

    ip: str
    """IP address of this node in this particular VPN."""

    def get_deploy_args(self) -> Dict:
        """
        Generate a dictionary of arguments that are required for the appropriate
        `Deploy` command of an exescript in order to pass the network configuration to the runtime
        on the provider's end.
        """
        deploy_args = {
            "net": [
                {
                    "id": self.network.network_id,
                    "ip": self.network.network_address,
                    "mask": self.network.netmask,
                    "nodeIp": self.ip,
                    "nodes": self.network.nodes_dict,
                }
            ]
        }
        return deploy_args

    def get_websocket_uri(self, port: int) -> str:
        """
        Get the websocket URI corresponding with a specific TCP port on this Node.

        :param port: TCP port of the service within the runtime
        :return: the url
        """
        net_api_ws = urlparse(self.network._net_api.api_url)._replace(scheme="ws").geturl()
        return f"{net_api_ws}/net/{self.network.network_id}/tcp/{self.ip}/{port}"


class NetworkState(StateMachine):
    """State machine describing the states and lifecycle of a :class:`Network` instance."""

    # states
    initialized = State("initialized", initial=True)
    creating = State("creating")
    ready = State("ready")
    removing = State("removing")
    removed = State("removed")

    # state-altering transitions (lifecycle)
    create = initialized.to(creating)
    start = creating.to(ready)
    stop = ready.to(removing)
    remove = removing.to(removed)

    # same-state transitions
    add_owner_address = creating.to.itself() | ready.to.itself()
    add_node = ready.to.itself()
    get_id = creating.to.itself() | ready.to.itself() | removing.to.itself()


class Network:
    """
    Describes a VPN created between the requestor and the provider nodes within Golem Network.
    """

    @classmethod
    async def create(
        cls,
        net_api: "yapapi.rest.net.Net",
        ip: str,
        owner_id: str,
        owner_ip: Optional[str] = None,
        mask: Optional[str] = None,
        gateway: Optional[str] = None,
    ) -> "Network":
        """Create a new VPN.

        :param net_api: the mid-level binding used directly to perform calls to the REST API.
        :param ip: the IP address of the network. May contain netmask, e.g. "192.168.0.0/24"
        :param owner_id: the node ID of the owner of this VPN (the requestor)
        :param owner_ip: the desired IP address of the requestor node within the newly-created network
        :param mask: Optional netmask (only if not provided within the `ip` argument)
        :param gateway: Optional gateway address for the network
        """

        network = cls(net_api, ip, owner_id, owner_ip, mask, gateway)
        network._state_machine.create()

        # create the network in yagna and set the id
        network._network_id = await net_api.create_network(
            network.network_address, network.netmask, network.gateway
        )
        logger.info("Created network: %s", network)

        # add requestor's own address to the network
        await network.add_owner_address(network.owner_ip)

        network._state_machine.start()
        return network

    def __init__(
        self,
        net_api: "yapapi.rest.net.Net",
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
        :param owner_ip: the desired IP address of the requestor node within the newly-created network
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
        self._owner_ip: IpAddress = ip_address(owner_ip) if owner_ip else self._next_address()
        self._state_machine: NetworkState = NetworkState()

        self._nodes: Dict[str, Node] = dict()
        """the mapping between a Golem node id and a Node in this VPN."""

        self._nodes_lock = asyncio.Lock()
        """an asyncio lock used to synchronize access to the `_nodes` mapping."""

    def __str__(self) -> str:
        return (
            f"Network {{ id: {self._network_id}, ip: {self.network_address}, mask: {self.netmask}}}"
        )

    async def __aenter__(self) -> "Network":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.remove()

    @property
    def owner_ip(self) -> str:
        """The IP address of the requestor node within the network."""
        return str(self._owner_ip)

    @property
    def state(self) -> State:
        """Current state in this network's lifecycle."""
        return self._state_machine.current_state

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
    def network_id(self) -> str:
        """The automatically-generated, unique ID of this VPN."""
        self._state_machine.get_id()
        assert self._network_id
        return self._network_id

    def _ensure_ip_in_network(self, ip: str):
        """Ensure the given IP address belongs to the network address range within this VPN."""
        if ip_address(ip) not in self._ip_network:
            raise NetworkError(
                f"The given IP ('{ip}') address must belong to the network ('{self._ip_network.with_netmask}')."
            )

    def _ensure_ip_unique(self, ip: str):
        """Ensure the given IP address has not already been assigned in this network."""
        if ip in self.nodes_dict:
            raise NetworkError(f"'{ip}' has already been assigned in this network.")

    async def add_owner_address(self, ip: str):
        """Assign the given IP address to the requestor in the network.

        :param ip: the IP address to assign to the requestor node.
        """
        self._state_machine.add_owner_address()
        self._ensure_ip_in_network(ip)

        async with self._nodes_lock:
            self._ensure_ip_unique(ip)
            self._nodes[self._owner_id] = Node(network=self, node_id=self._owner_id, ip=ip)

        await self._net_api.add_address(self.network_id, ip)

    async def add_node(self, node_id: str, ip: Optional[str] = None) -> Node:
        """Add a new node to the network.

        :param node_id: Node ID within the Golem network of this VPN node.
        :param ip: IP address to assign to this node.
        """
        self._state_machine.add_node()

        async with self._nodes_lock:
            if ip:
                self._ensure_ip_in_network(ip)
                self._ensure_ip_unique(ip)
            else:
                while True:
                    ip = str(self._next_address())
                    if ip not in self.nodes_dict:
                        break

            node = Node(network=self, node_id=node_id, ip=ip)
            self._nodes[node_id] = node

        await self._net_api.add_node(self.network_id, node_id, ip)

        return node

    async def _refresh_node(self, node: Node):
        logger.debug("refreshing node %s", node)
        try:
            await self._net_api.add_node(self.network_id, node.node_id, node.ip)
        except ApiException as e:
            # the `409 Conflict` shouldn't happen with the latest yagna (0.8.0-rc7)
            # but we're adding the work around as the error is mostly harmless anyway
            if e.status == 409:
                logger.warning(
                    "Could not refresh node %s (%s). network_id=%s",
                    node.node_id,
                    node.ip,
                    self.network_id,
                )
            else:
                raise

    async def refresh_nodes(self):
        await asyncio.gather(*[self._refresh_node(n) for n in self._nodes.values()])

    async def remove(self) -> None:
        """Remove this network, terminating any connections it provides."""
        self._state_machine.stop()
        try:
            await self._net_api.remove_network(self.network_id)
            logger.info("Removed network: %s", self)
        except ApiException as e:
            if e.status == 404:
                logger.warning(
                    "Tried removing a network which doesn't exist. network_id=%s", self.network_id
                )
        self._state_machine.remove()

    def _next_address(self) -> IpAddress:
        """Provide the next available IP address within this Network.

        :raises NetworkError in case the network ran out of available addresses.
        """
        try:
            return next(self._hosts)  # type: ignore
        except StopIteration:
            raise NetworkError(f"No more addresses available in '{self._ip_network.with_netmask}'.")


class NetworkError(Exception):
    """Exception raised by :class:`Network` when an operation is not possible"""
