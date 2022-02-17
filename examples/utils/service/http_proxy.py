import abc
import aiohttp
from aiohttp import web
import asyncio
import functools
import re

from yapapi.services import Cluster, ServiceState, Service
from .. import (
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_GREEN,
)


class HttpProxyService(Service, abc.ABC):

    async def handle_request(self, request: web.Request):
        """
        handle the request coming from the local HTTP server
        by passing it to the instance through the VPN
        """
        instance_ws = self.network_node.get_websocket_uri(80)
        app_key = self.cluster.service_runner._job.engine._api_config.app_key  # noqa
        query_string = request.path_qs

        print(
            f"{TEXT_COLOR_GREEN}"
            f"sending a remote request '{query_string}' to {self}"
            f"{TEXT_COLOR_DEFAULT}"
        )
        ws_session = aiohttp.ClientSession()
        async with ws_session.ws_connect(
            instance_ws, headers={"Authorization": f"Bearer {app_key}"}
        ) as ws:
            await ws.send_str(f"GET {query_string} HTTP/1.0\r\n\r\n")
            headers = await ws.__anext__()
            status = int(re.match("^HTTP/1.1 (\\d+)", headers.data.decode("ascii")).group(1))
            print(f"{TEXT_COLOR_GREEN}remote headers: {headers.data} {TEXT_COLOR_DEFAULT}")

            if status == 200:
                content = await ws.__anext__()
                data: bytes = content.data

                print(f"{TEXT_COLOR_GREEN}remote content: {data} {TEXT_COLOR_DEFAULT}")

                response_text = data.decode("utf-8")
            else:
                response_text = None
            print(
                f"{TEXT_COLOR_GREEN}local response ({status}): {response_text}{TEXT_COLOR_DEFAULT}"
            )

        await ws_session.close()
        return response_text, status


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
        response, status = await instance.handle_request(request)
        return web.Response(text=response, status=status)

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
