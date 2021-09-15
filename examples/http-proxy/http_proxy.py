#!/usr/bin/env python3
"""
the requestor agent controlling and interacting with the "simple service"
"""
import asyncio
from datetime import datetime, timedelta, timezone
import pathlib
import sys


from yapapi import (
    __version__ as yapapi_version,
)
from yapapi import Golem
from yapapi.services import Service

from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
    TEXT_COLOR_MAGENTA,
    format_usage,
    run_golem_example,
)

STARTING_TIMEOUT = timedelta(minutes=4)


class HttpService(Service):

    def __repr__(self):
        return f"<{self.__class__.__name__} on {self.provider_name}>"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="de6d4cf997138af384caf32b1d222f35ead1d3639eebafa575359f1d",
        )

    async def start(self):
        self._ctx._started = True
        async for script in super().start():
            yield script

        self._ctx.run("/docker-entrypoint.sh")
        self._ctx.run("/bin/chmod", "a+x", "/")
        self._ctx.run("/usr/sbin/nginx"),
        yield self._ctx.commit()

    async def run(self):
        app_key = self.cluster._engine._api_config.app_key
        http_ws = self.network_node.get_websocket_uri(80)

        print(
            "Connect using:\n"
            f"{TEXT_COLOR_CYAN}"
            f"websocat {http_ws} --binary -H=Authorization:\"Bearer {app_key}\""
            f"{TEXT_COLOR_DEFAULT}"
        )

        # await indefinitely...
        await asyncio.Future()
        yield


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

        network = await golem.create_network("192.168.0.1/24")
        cluster = await golem.run_service(HttpService, network=network, num_instances=num_instances)

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        def still_running():
            return any([s for s in cluster.instances if s.is_available])

        while True:
            print(instances())
            try:
                await asyncio.sleep(5)
            except (KeyboardInterrupt, asyncio.CancelledError):
                break

        cluster.stop()

        cnt = 0
        while cnt < 3 and still_running():
            print(instances())
            await asyncio.sleep(5)
            cnt += 1


if __name__ == "__main__":
    parser = build_parser("An extremely simple http proxy")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"http-proxy-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(subnet_tag=args.subnet_tag, driver=args.driver, network=args.network, ),
        log_file=args.log_file,
    )
