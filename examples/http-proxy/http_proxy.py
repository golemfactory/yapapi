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
import re
import shlex
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
    print_env_info,
)

STARTING_TIMEOUT = timedelta(minutes=4)


# ######## local http server

request_count = 0


async def request_handler(cluster: Cluster, request: web.Request):
    global request_count

    print(f"{TEXT_COLOR_GREEN}local HTTP request: {dict(request.query)}{TEXT_COLOR_DEFAULT}")

    instance: HttpService = cluster.instances[request_count % len(cluster.instances)]
    request_count += 1
    response, status = await instance.handle_request(request.path_qs)
    return web.Response(text=response, status=status)


async def run_local_server(cluster: Cluster, port: int):
    """
    run a local HTTP server, listening on `port`
    and passing all requests through the `request_handler` function above
    """
    handler = functools.partial(request_handler, cluster)
    runner = web.ServerRunner(web.Server(handler))
    await runner.setup()
    site = web.TCPSite(runner, port=port)
    await site.start()

    return site


# ######## Golem Service


class HttpService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="16ad039c00f60a48c76d0644c96ccba63b13296d140477c736512127",
            # we're adding an additional constraint to only select those nodes that
            # are offering VPN-capable VM runtimes so that we can connect them to the VPN
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def start(self):
        # perform the initialization of the Service
        # (which includes sending the network details within the `deploy` command)
        async for script in super().start():
            yield script

        # start the remote HTTP server and give it some content to serve in the `index.html`
        script = self._ctx.new_script()
        script.run("/docker-entrypoint.sh")
        script.run("/bin/chmod", "a+x", "/")
        msg = f"Hello from inside Golem!\n... running on {self.provider_name}"
        script.run(
            "/bin/sh",
            "-c",
            f"echo {shlex.quote(msg)} > /usr/share/nginx/html/index.html",
        )
        script.run("/bin/rm", "/var/log/nginx/access.log", "/var/log/nginx/error.log")
        script.run("/usr/sbin/nginx"),
        yield script

    async def shutdown(self):
        # grab the remote HTTP server's logs on shutdown
        # we don't need to display them in any way since the result of those commands
        # is written into the example's logfile alongside all other results

        script = self._ctx.new_script()
        script.run("/bin/cat", "/var/log/nginx/access.log")
        script.run("/bin/cat", "/var/log/nginx/error.log")
        yield script

    # we don't need to implement `run` since, after the service is started,
    # all communication is performed through the VPN

    async def handle_request(self, query_string: str):
        """
        handle the request coming from the local HTTP server
        by passing it to the instance through the VPN
        """
        instance_ws = self.network_node.get_websocket_uri(80)
        app_key = self.cluster._engine._api_config.app_key

        print(
            f"{TEXT_COLOR_GREEN}sending a remote request '{query_string}' to {self}{TEXT_COLOR_DEFAULT}"
        )
        ws_session = aiohttp.ClientSession()
        async with ws_session.ws_connect(
            instance_ws, headers={"Authorization": f"Bearer {app_key}"}
        ) as ws:
            await ws.send_str(f"GET {query_string} HTTP/1.0\r\n\r\n")
            headers = await ws.__anext__()
            status = int(re.match("^HTTP/1.1 (\d+)", headers.data.decode("ascii")).group(1))
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


# ######## Main application code which spawns the Golem service and the local HTTP server


async def main(
    subnet_tag,
    payment_driver=None,
    payment_network=None,
    num_instances=1,
    port=8080,
):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        commissioning_time = datetime.now()

        network = await golem.create_network("192.168.0.1/24")
        cluster = await golem.run_service(HttpService, network=network, num_instances=num_instances)

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        def still_starting():
            return len(cluster.instances) < num_instances or any(
                s.state == ServiceState.starting for s in cluster.instances
            )

        # wait until all remote http instances are started

        while still_starting() and datetime.now() < commissioning_time + STARTING_TIMEOUT:
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

        if still_starting():
            raise Exception(
                f"Failed to start instances after {STARTING_TIMEOUT.total_seconds()} seconds"
            )

        # service instances started, start the local HTTP server

        site = await run_local_server(cluster, port)

        print(
            f"{TEXT_COLOR_CYAN}Local HTTP server listening on:\nhttp://localhost:{port}{TEXT_COLOR_DEFAULT}"
        )

        # wait until Ctrl-C

        while True:
            print(instances())
            try:
                await asyncio.sleep(10)
            except (KeyboardInterrupt, asyncio.CancelledError):
                break

        # perform the shutdown of the local http server and the service cluster

        await site.stop()
        print(f"{TEXT_COLOR_CYAN}HTTP server stopped{TEXT_COLOR_DEFAULT}")

        cluster.stop()

        cnt = 0
        while cnt < 3 and any(s.is_available for s in cluster.instances):
            print(instances())
            await asyncio.sleep(5)
            cnt += 1

        await network.remove()


if __name__ == "__main__":
    parser = build_parser("An extremely simple http proxy")
    parser.add_argument(
        "--num-instances",
        type=int,
        default=2,
        help="The number of instances of the http service to spawn",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="The local port to listen on",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"http-proxy-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            num_instances=args.num_instances,
            port=args.port,
        ),
        log_file=args.log_file,
    )
