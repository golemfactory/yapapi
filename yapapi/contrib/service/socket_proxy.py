"""TCP socket proxy.

A local proxy that facilitates connections to any VPN-enabled TCP services launched on Golem
providers using yapapi's Services API.

For usage in a complete requestor agent app, see the
`ssh <https://github.com/golemfactory/yapapi/tree/master/examples/ssh>`_ example in the
yapapi repository.
"""
import abc
import asyncio
import itertools
import logging
from typing import Dict, Iterator, List

import aiohttp
from typing_extensions import Final

from yapapi.services import Cluster, Service

from . import DEFAULT_TIMEOUT, WEBSOCKET_CHUNK_LIMIT
from .chunk import chunks

logger = logging.getLogger(__name__)


DEFAULT_SOCKET_BUFFER_SIZE: Final[int] = 1024 * 1024
DEFAULT_SOCKET_ADDRESS: Final[str] = "127.0.0.1"


connection_ids: Iterator[int] = itertools.count(1)
"""An iterator providing incremental integer IDs to Proxy Connections."""


class SocketProxyService(Service, abc.ABC):
    """Base class for services connected to the :class:`~SocketProxy`.

    Implements the interface required by the `SocketProxy`.
    """

    remote_ports: List[int]
    """List of remote ports to open the proxies for when using \
    `SocketProxy.`:meth:`~SocketProxy.run`."""


class ProxyConnection:
    """A single connection between a local TPC socket and a remote websocket.

    Receives a reader/writer pair for the local connection and creates a client session
    on the remote websocket to forward data between the two, using two asyncio tasks:
    the sender and the responder.
    """

    send_reader: asyncio.StreamReader
    send_writer: asyncio.StreamWriter
    ws: aiohttp.ClientWebSocketResponse

    def __init__(
        self,
        server: "ProxyServer",
        listen_reader: asyncio.StreamReader,
        listen_writer: asyncio.StreamWriter,
        buffer_size: int = DEFAULT_SOCKET_BUFFER_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.server = server
        self.listen_reader = listen_reader
        self.listen_writer = listen_writer
        self.buffer_size = buffer_size
        self.timeout = timeout

        self.to_data_len = 0
        self.from_data_len = 0
        self._done = False
        self._tasks: List[asyncio.Task] = list()

        self.ws_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        self._id = next(connection_ids)

    def __repr__(self):
        return f"{self.__class__.__name__} {self._id}"

    def _cancel_tasks(self):
        for t in self._tasks:
            t.cancel()

    async def sender(self):
        logger.debug("%s: starting the sender.", self)
        while not self._done:
            try:
                client_data = await self.listen_reader.read(self.buffer_size)
                if not client_data:
                    logger.debug("%s: client socket closed.", self)
                    self._done = True
                    self._cancel_tasks()
                    break

                logger.debug("%s: received %s bytes from the client.", self, len(client_data))
                self.to_data_len += len(client_data)

                for chunk in chunks(memoryview(client_data), WEBSOCKET_CHUNK_LIMIT):
                    await self.ws.send_bytes(chunk)

                logger.debug("%s: sent %s bytes to the remote end.", self, len(client_data))
            except asyncio.CancelledError:
                pass

        logger.debug("%s: sender stopped.", self)

    async def responder(self):
        logger.debug("%s: starting the responder.", self)
        while not self._done:
            try:
                remote_response = await self.ws.receive(timeout=self.timeout)
                if remote_response.type == aiohttp.WSMsgType.CLOSED:
                    logger.debug("%s: received `CLOSED` from the remote end", self)
                    self._done = True
                    self._cancel_tasks()
                    break

                remote_data = remote_response.data

                logger.debug("%s: received %s bytes from the remote end", self, len(remote_data))

                self.listen_writer.write(remote_data)
                await self.listen_writer.drain()

                logger.debug("%s: sent %s bytes to the client", self, len(remote_data))

                self.from_data_len += len(remote_data)
            except asyncio.CancelledError:
                pass

        logger.debug("%s: responder stopped", self)

    async def run(self):
        client_addr = self.listen_writer.get_extra_info("peername")
        logger.info("%s: client connection from: %s, on: %s", self, client_addr, self.server)

        async with self.ws_session.ws_connect(
            self.server.instance_ws,
            headers={"Authorization": f"Bearer {self.server.app_key}"},
        ) as self.ws:
            self._tasks.extend(
                [
                    asyncio.create_task(self.sender()),
                    asyncio.create_task(self.responder()),
                ]
            )
            try:
                await asyncio.gather(*self._tasks)
            except asyncio.CancelledError:
                pass
        await self.ws_session.close()
        self.close()

    def close(self):
        self.listen_writer.close()
        logger.info(
            "%s: connection closed. sent: %s, received: %s",
            self,
            self.to_data_len,
            self.from_data_len,
        )


class ProxyServer:
    """A server for a pair of local/remote ports within the :class:`~SocketProxy`.

    Connects a local TPC socket with a remote websocket on a single
    :class:`~Service` instance.
    """

    def __init__(
        self,
        proxy: "SocketProxy",
        service: Service,
        remote_port: int,
        local_address: str,
        local_port: int,
        buffer_size: int = DEFAULT_SOCKET_BUFFER_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.proxy = proxy
        self.service = service
        self.remote_port = remote_port
        self.local_address = local_address
        self.local_port = local_port
        self.buffer_size = buffer_size
        self.timeout = timeout
        self._running = False

    def __repr__(self):
        return (
            f"{self.__class__.__name__} "
            f"{self.local_address}:{self.local_port} -> "
            f"{self.service} [{self.instance_ws}]"
        )

    @property
    def app_key(self):
        """Return the application key used to authorize access to `yagna`'s REST API."""
        return (
            self.service.cluster.service_runner._job.engine._api_config.app_key  # type: ignore[union-attr]  # noqa
        )

    @property
    def instance_ws(self):
        """Return the websocket URI for the specific remote port of the Service."""
        return self.service.network_node.get_websocket_uri(self.remote_port)  # type: ignore[union-attr]  # noqa

    async def handler(
        self, listen_reader: asyncio.StreamReader, listen_writer: asyncio.StreamWriter
    ):
        connection = ProxyConnection(
            self, listen_reader, listen_writer, buffer_size=self.buffer_size, timeout=self.timeout
        )
        await connection.run()

    async def run(self):
        """Run the asyncio TCP server for a single port on a Service.

        Preferred usage is through `:meth:`~SocketProxy.run_server`.
        """
        server = await asyncio.start_server(self.handler, self.local_address, self.local_port)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)  # type: ignore  # noqa
        logger.info("Listening on: %s, forwarding to: %s", addrs, self.instance_ws)

        try:
            async with server:
                self._running = True
                await server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False


class SocketProxy:
    """Exposes ports of services running in VMs on providers as local ports.

    The connections can be routed to instances of services connected to a Golem VPN
    using `yapapi`'s Network API (:meth:`~yapapi.Golem.create_network`).

    Example usage::

        class SomeService(SocketProxyService):
            def __init__(self, remote_port: int = 4242):
                super().init()
                self.remote_ports = [remote_port]

        ...

        cluster = await golem.run_service(
            SomeService,
            network=network,
        )

        # ensure services are started

        ...

        proxy = SocketProxy(ports=[8484])
        await proxy.run(cluster)

        ... # connections to local port 8484 will be routed to port 4242 within the VM

        await proxy.stop()
        cluster.stop()


    Example usage directly from a Service handler::

        class SomeOtherService(SocketProxyService):
            remote_port = 22  # e.g. an SSH daemon

            def __init__(self, proxy: SocketProxy):
                super().__init__()
                self.proxy = proxy

            async def start(self):
                # perform the initialization of the Service

                ...

                server = await self.proxy.run_server(self, self.remote_port)


        proxy = SocketProxy(ports=[2222])

        cluster = await golem.run_service(
            SomeOtherService,
            network=network,
            instance_params=[{"proxy": proxy}],
        )

        ... # connections to local port 2222 will be routed to port 22 within the VM

        await proxy.stop()
        cluster.stop()

    """

    servers: Dict[Service, Dict[int, ProxyServer]]

    def __init__(
        self,
        ports: List[int],
        address: str = DEFAULT_SOCKET_ADDRESS,
        buffer_size: int = DEFAULT_SOCKET_BUFFER_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """Initialize the TCP socket proxy service.

        :param ports: a list of local ports that will be assigned to consecutive connections
        :param address: the IP address to bind the local server to
        :param buffer_size: the size of the data buffer used for the connections
        :param timeout: the timeout in seconds for the response of the remote end
        """
        self.ports = ports
        self.address = address
        self.buffer_size = buffer_size
        self.timeout = timeout

        self.servers = dict()
        self._available_ports = list(ports)
        self._tasks: List[asyncio.Task] = list()

    async def run_server(self, service: Service, remote_port: int):
        """Run a socket proxy for a single port on a instance of a service.

        :param service: the service instance
        :param remote_port: the remote port on which a TCP service is listening on the remote end
        """
        assert service.network_node, "Service must be started on a VPN."

        local_port = self._available_ports.pop(0)
        logger.info("Starting proxy server for %s (%s -> %s)", service, local_port, remote_port)
        server = ProxyServer(
            self,
            service,
            remote_port,
            self.address,
            local_port,
            buffer_size=self.buffer_size,
            timeout=self.timeout,
        )

        self._tasks.append(asyncio.create_task(server.run()))

        service_servers = self.servers.setdefault(service, dict())
        service_servers[remote_port] = server

        return server

    def get_server(self, service: Service, remote_port: int):
        """Get a running server for a given service and port."""
        return self.servers[service][remote_port]

    async def run(self, cluster: Cluster[SocketProxyService]):
        """Run the proxy servers for all ports on a cluster.

        :param cluster: the cluster for which the proxy connections should be enabled.
        """
        for service in cluster.instances:
            for remote_port in service.remote_ports:
                await self.run_server(service, remote_port)

    async def stop(self):
        """Stop servers for all connections."""
        logger.info("Stopping socket proxy...")
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks)
        logger.info("Socket proxy stopped.")
