import abc
import aiohttp
from aiohttp import web
import asyncio
from multidict import CIMultiDict
import re
from typing import Optional, Tuple

from yapapi.services import Cluster, ServiceState, Service
from .. import (
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_GREEN,
    TEXT_COLOR_RED,
)


class HttpProxyService(Service, abc.ABC):
    def __init__(
        self, remote_port: int = 80, remote_host: Optional[str] = None, response_timeout: float = 60
    ):
        super().__init__()
        self._remote_port = remote_port
        self._remote_host = remote_host
        self._remote_response_timeout = response_timeout

    @staticmethod
    def _extract_response(content: bytes) -> Tuple[int, CIMultiDict, bytes]:
        m = re.match(b"^(.*?)\r\n\r\n(.*)$", content, re.DOTALL)

        if not m:
            return 500, CIMultiDict(), b"Error extracting remote request"

        header_lines = m.group(1).decode("ascii").splitlines()
        status = int(header_lines[0].split()[1])

        headers = CIMultiDict()
        for header_line in header_lines[1:]:
            if header_line:
                name, value = header_line.split(": ", maxsplit=1)
                headers[name] = value

        body = m.group(2)
        return status, headers, body

    async def handle_request(self, request: web.Request) -> web.Response:
        """
        handle the request coming from the local HTTP server
        by passing it to the instance through the VPN
        """
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
        ).encode("ascii") + (await request.read() if request.can_read_body else b"")

        print(
            f"{TEXT_COLOR_GREEN}"
            f"sending a remote request to `{request.path_qs}` @ {self} [{remote_request}]"
            f"{TEXT_COLOR_DEFAULT}"
        )

        ws_session = aiohttp.ClientSession()
        async with ws_session.ws_connect(
            instance_ws, headers={"Authorization": f"Bearer {app_key}"}
        ) as ws:
            await ws.send_bytes(remote_request)
            remote_content: bytes = b""

            for _ in range(2):
                try:
                    l = await ws.receive(timeout=self._remote_response_timeout)
                    remote_content += l.data
                except asyncio.exceptions.TimeoutError:
                    print(f"{TEXT_COLOR_RED}Remote HTTP server timeout{TEXT_COLOR_DEFAULT}")
                    return web.Response(status=500, text="Remote HTTP server timeout")

            status, headers, body = self._extract_response(remote_content)
            print(
                f"{TEXT_COLOR_GREEN}response: {status} headers:{headers} body:{body}{TEXT_COLOR_DEFAULT}"
            )

        await ws_session.close()
        return web.Response(status=status, body=body, headers=headers)


class LocalHttpProxy:
    """runs a local aiohttp server and processes requests through instances of HttpProxyService."""

    def __init__(self, cluster: Cluster[HttpProxyService], port: int):
        self._request_count = 0
        self._request_lock = asyncio.Lock()
        self._cluster = cluster
        self._port = port
        self._site = None

    async def request_handler(self, request: web.Request):
        print(f"{TEXT_COLOR_GREEN}local HTTP request: {dict(request.query)}{TEXT_COLOR_DEFAULT}")

        instances = [i for i in self._cluster.instances if i.state == ServiceState.running]
        async with self._request_lock:
            instance: HttpProxyService = instances[self._request_count % len(instances)]
            self._request_count += 1
        return await instance.handle_request(request)

    async def run(self):
        """
        run a local HTTP server, listening on `port`
        and passing all requests through the `request_handler` function above
        """
        runner = web.ServerRunner(web.Server(self.request_handler))  # type: ignore
        await runner.setup()
        site = web.TCPSite(runner, port=self._port)
        await site.start()

        self._site = site

    async def stop(self):
        await self._site.stop()
