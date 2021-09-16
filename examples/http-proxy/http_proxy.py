#!/usr/bin/env python3
"""
a simple http proxy example
"""
import asyncio

import aiohttp
from aiohttp import web
from datetime import datetime, timedelta
import functools
import pathlib
import sys


from yapapi import (
    __version__ as yapapi_version,
)
from yapapi import Golem
from yapapi.services import Service, Cluster, ServiceState

from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_GREEN,
    TEXT_COLOR_YELLOW,
    run_golem_example,
)

STARTING_TIMEOUT = timedelta(minutes=4)


# ######## local http server

async def web_root(cluster: Cluster, request: web.Request):
    print(f"{TEXT_COLOR_GREEN}local HTTP request: {dict(request.query)}{TEXT_COLOR_DEFAULT}")

    instance_ws = cluster.instances[0].network_node.get_websocket_uri(80)
    app_key = cluster._engine._api_config.app_key

    ws_session = aiohttp.ClientSession()
    async with ws_session.ws_connect(instance_ws, headers={"Authorization": f"Bearer {app_key}"}) as ws:
        await ws.send_str("GET / HTTP/1.0\n\n")
        headers = await ws.__anext__()
        print(f"{TEXT_COLOR_GREEN}remote headers: {headers.data} {TEXT_COLOR_DEFAULT}")
        content = await ws.__anext__()
        data: bytes = content.data
        print(f"{TEXT_COLOR_GREEN}remote content: {data} {TEXT_COLOR_DEFAULT}")
        
        # todo pass headers to the response ? ...

    return web.Response(text=content.data.decode("utf-8"))


async def run_local_server(cluster: Cluster, port: int):
    runner = web.ServerRunner(web.Server(functools.partial(web_root, cluster)))
    await runner.setup()
    site = web.TCPSite(runner, port=port)
    await site.start()

    return site


# ######## Golem Service

class HttpService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="de6d4cf997138af384caf32b1d222f35ead1d3639eebafa575359f1d",
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def start(self):
        self._ctx._started = True
        async for script in super().start():
            yield script

        self._ctx.run("/docker-entrypoint.sh")
        self._ctx.run("/bin/chmod", "a+x", "/")
        self._ctx.run("/usr/sbin/nginx"),
        yield self._ctx.commit()


# ######## Main application code which spawns the Golem service and the local HTTP server

async def main(
    subnet_tag, driver=None, network=None, num_instances=1,
):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
    ) as golem:

        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{golem.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{golem.network}{TEXT_COLOR_DEFAULT}\n"
        )

        commissioning_time = datetime.now()

        network = await golem.create_network("192.168.0.1/24")
        cluster = await golem.run_service(HttpService, network=network, num_instances=num_instances)

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        def still_running():
            return any([s for s in cluster.instances if s.is_available])

        def still_starting():
            return len(cluster.instances) < num_instances or any(
                [s for s in cluster.instances if s.state == ServiceState.starting]
            )

        # wait until instances are started

        while still_starting() and datetime.now() < commissioning_time + STARTING_TIMEOUT:
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

        # service instances started, start the local HTTP server

        port = 8080
        site = await run_local_server(cluster, port)

        print(f"{TEXT_COLOR_CYAN}Local HTTP server listening on:\nhttp://localhost:{port}{TEXT_COLOR_DEFAULT}")

        # wait until Ctrl-C

        while True:
            print(instances())
            try:
                await asyncio.sleep(10)
            except (KeyboardInterrupt, asyncio.CancelledError):
                break

        # perform the shutdown of the local server and

        cluster.stop()

        cnt = 0
        while cnt < 3 and still_running():
            print(instances())
            await asyncio.sleep(5)
            cnt += 1

        await site.stop()
        print(f"{TEXT_COLOR_CYAN}HTTP server stopped{TEXT_COLOR_DEFAULT}")


if __name__ == "__main__":
    parser = build_parser("An extremely simple http proxy")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"http-proxy-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(subnet_tag=args.subnet_tag, driver=args.driver, network=args.network, ),
        log_file=args.log_file,
    )
