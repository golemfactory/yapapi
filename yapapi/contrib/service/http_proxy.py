"""
Local HTTP Proxy
^^^^^^^^^^^^^^^^

A local HTTP proxy that enables easy connections to any VPN-enabled, HTTP-based services launched on
Golem providers using yapapi's Services API.

For usage in a complete requestor agent app, see the
`http-proxy <https://github.com/golemfactory/yapapi/tree/master/examples/http-proxy>`_ and
`webapp <https://github.com/golemfactory/yapapi/tree/master/examples/webapp>`_ examples in the
yapapi repository.
"""
import abc
import aiohttp
from aiohttp import web, client_ws
import asyncio
import logging
from multidict import CIMultiDict
import re
from typing import Optional
import traceback

from yapapi.services import Cluster, ServiceState, Service


logger = logging.getLogger(__name__)


class _ResponseParser:
    def __init__(self, ws: client_ws.ClientWebSocketResponse, timeout: float = 10.0):
        self.headers_data = b""
        self.headers: Optional[CIMultiDict] = None
        self.content = b""
        self.status: Optional[int] = None
        self.ws = ws
        self.timeout = timeout

    def process_headers(self):
        header_lines = self.headers_data.decode("ascii").splitlines()

        self.status = int(header_lines[0].split()[1])
        self.headers = CIMultiDict()

        for line in header_lines[1:]:
            if line:
                name, value = line.split(": ", maxsplit=1)
                self.headers[name] = value

    def receive_data(self, data: bytes):
        if self.status:
            self.content += data
        else:
            parts = re.split(b"\r\n\r\n", data)
            self.headers_data += parts.pop(0)
            if parts:
                self.content += parts.pop(0)
                self.process_headers()

    @property
    def content_received(self) -> bool:
        if not self.status:
            return False

        assert self.headers is not None
        content_length = self.headers.get("Content-Length")

        if not content_length:
            return True

        return len(self.content) >= int(content_length)

    async def get_response(self) -> web.Response:
        while not self.content_received:
            ws_response = await self.ws.receive(self.timeout)
            self.receive_data(ws_response.data)

        assert self.status
        return web.Response(status=self.status, headers=self.headers, body=self.content)


class HttpProxyService(Service, abc.ABC):
    """
    Base class for services connected to the :class:`~LocalHttpProxy`.

    Implements the interface used by `LocalHttpProxy` to route HTTP requests to the instances of an
    HTTP service running on providers.
    """

    def __init__(
        self,
        remote_port: int = 80,
        remote_host: Optional[str] = None,
        response_timeout: float = 10.0,
    ):
        """
        Initialize the HTTP proxy service

        :param remote_port: the port on which the service on the provider's end listens
        :param remote_host: optional hostname to be used in the headers passed to the remote HTTP
            server. If not provided, the `Host` header of the incoming http requests will be passed.
        :param response_timeout: the timeout for the requests made to the remote server
        """
        super().__init__()
        self._remote_port = remote_port
        self._remote_host = remote_host
        self._remote_response_timeout = response_timeout

    async def handle_request(self, request: web.Request) -> web.Response:
        """
        handle a single request coming from a :class:`~LocalHttpProxy` server
        by passing it to the HTTP service on the provider's end through the VPN

        :param request: an `aiohttp.web.Request`
        :return: an `aiohttp.web.Response`
        """
        assert self.network_node, "Ensure that the service is started within a network."
        assert self.cluster

        instance_ws = self.network_node.get_websocket_uri(self._remote_port)
        app_key = self.cluster.service_runner._job.engine._api_config.app_key  # noqa

        remote_headers = "\r\n".join(
            [
                f"{k}: {v if k != 'Host' else self._remote_host or v}"
                for k, v in request.headers.items()
            ]
        )

        remote_request: bytes = (
            f"{request.method} {request.path_qs} "
            f"HTTP/{request.version.major}.{request.version.minor}\r\n"
            f"{remote_headers}\r\n\r\n"
        ).encode("ascii")

        if request.can_read_body:
            remote_request += await request.read()

        logger.info("Sending request: `%s %s` to %s", request.method, request.path_qs, self)
        logger.debug("remote_request: %s", remote_request)

        ws_session = aiohttp.ClientSession()
        async with ws_session.ws_connect(
            instance_ws,
            headers={"Authorization": f"Bearer {app_key}"},
        ) as ws:
            await ws.send_bytes(remote_request)

            response_parser = _ResponseParser(ws, self._remote_response_timeout)
            try:
                response = await response_parser.get_response()
                logger.info("Remote response received. status=%s", response.status)
                logger.debug(
                    "Remote response: status=%s, headers=%s, body=%s",
                    response.status,
                    response.headers,
                    response.body,
                )
            except Exception as e:
                logger.error(
                    "Error receiving remote response. url=%s, service=%s, exception=%s(%s) [%s])",
                    request.path_qs,
                    self,
                    type(e),
                    str(e),
                    traceback.format_exc(),
                )
                response = web.Response(status=500, text="Error retrieving the remote response.")

        await ws_session.close()

        return response


class LocalHttpProxy:
    """Runs a local `aiohttp` server and processes requests through instances of
    :class:`~HttpProxyService`.

    Using `yapapi`'s Network API (:meth:`~yapapi.Golem.create_network`), execution units on the
    provider nodes can be connected to a virtual network which can then be used both for
    communication between those nodes (through virtual network interfaces within VMs) and between
    the specific nodes and the requestor agent (through a websocket endpoint in the yagna daemon's
    REST API).

    `LocalHttpProxy` and `HttpProxyService` use the latter to enable HTTP connections to be routed
    from a local port on the requestor's host, to a specified TCP port within the VM on the
    provider's end.

    Example usage::

        class HttpService(HttpProxyService):
            ...

        cluster = await golem.run_service(
            HttpService,
            network=network,
            instance_params=[{"remote_port": 80}],  # optional, 80 by default
        )

        proxy = LocalHttpProxy(cluster, 8080)
        await proxy.run()

        ... # requests made to http://localhost:8080 are routed to port 80 within the VM

        await proxy.stop()
        cluster.stop()

    """

    def __init__(self, cluster: Cluster[HttpProxyService], port: int):
        """
        Initialize the local HTTP proxy

        :param cluster: a :class:`~yapapi.services.Cluster` of one or more VPN-connected
            :class:`~HttpProxyService` instances.
        :param port: a local port on the requestor's machine to listen on
        """
        self._request_count = 0
        self._request_lock = asyncio.Lock()
        self._cluster = cluster
        self._port = port
        self._site: Optional[web.TCPSite] = None

    async def _request_handler(self, request: web.Request) -> web.Response:
        logger.info("Received a local HTTP request: %s %s", request.method, request.path_qs)

        instances = [i for i in self._cluster.instances if i.state == ServiceState.running]

        if not instances:
            logger.error(
                "No running instances of %s available to handle the request", self._cluster
            )
            return web.Response(status=503, text="No service instances available.")

        async with self._request_lock:
            instance: HttpProxyService = instances[self._request_count % len(instances)]
            self._request_count += 1
        return await instance.handle_request(request)

    async def run(self):
        """
        run a local HTTP server, listening on the specified port and passing subsequent requests to
        the :meth:`~HttpProxyService.handle_request` of the specified cluster in a round-robin
        fashion
        """
        runner = web.ServerRunner(web.Server(self._request_handler))  # type: ignore
        await runner.setup()
        site = web.TCPSite(runner, port=self._port)
        await site.start()

        self._site = site

    async def stop(self):
        assert self._site, "Not started, call `run` first."
        await self._site.stop()
