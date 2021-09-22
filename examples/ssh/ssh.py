#!/usr/bin/env python3
import asyncio
import pathlib
import sys
from uuid import uuid4


from datetime import datetime

from yapapi import (
    Golem,
    __version__ as yapapi_version,
)
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.payload import vm
from yapapi.services import Service

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
    run_golem_example,
)


class SshService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="ea233c6774b1621207a48e10b46e3e1f944d881911f499f5cbac546a",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def run(self):
        connection_uri = self.network_node.get_websocket_uri(22)
        app_key = self.cluster._engine._api_config.app_key

        self._ctx.run("/bin/bash", "-c", "syslogd")
        self._ctx.run("/bin/bash", "-c", "ssh-keygen -A")
        self._ctx.run("/bin/bash", "-c", "/usr/sbin/sshd")
        yield self._ctx.commit()

        print(
            "Connect with:\n"
            f"{TEXT_COLOR_CYAN}"
            f"ssh -o ProxyCommand='websocat asyncstdio: {connection_uri} --binary -H=Authorization:\"Bearer {app_key}\"' root@{uuid4().hex}"
            f"{TEXT_COLOR_DEFAULT}"
        )

        # await indefinitely...
        await asyncio.Future()


async def main(subnet_tag, driver=None, network=None):
    # By passing `event_consumer=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
    ) as golem:

        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{golem.subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{golem.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{golem.network}{TEXT_COLOR_DEFAULT}\n"
        )

        network = await golem.create_network("192.168.0.1/24")
        cluster = await golem.run_service(SshService, network=network, num_instances=2)

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        while True:
            print(instances())
            try:
                await asyncio.sleep(5)
            except (KeyboardInterrupt, asyncio.CancelledError):
                break

        cluster.stop()

        cnt = 0
        while cnt < 3 and cluster.has_active_instances:
            print(instances())
            await asyncio.sleep(5)
            cnt += 1


if __name__ == "__main__":
    parser = build_parser("Golem VPN SSH example")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"ssh-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(subnet_tag=args.subnet_tag, driver=args.driver, network=args.network),
        log_file=args.log_file,
    )
