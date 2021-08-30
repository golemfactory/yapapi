#!/usr/bin/env python3
import asyncio
import pathlib
import sys
from typing import Optional

from datetime import datetime, timedelta

from yapapi import (
    Golem,
    NoPaymentAccountError,
    __version__ as yapapi_version,
    WorkContext,
    windows_event_loop_fix,
)
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.payload import vm
from yapapi.rest.net import Network, Node
from yapapi.services import Service, Cluster

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)


class SshService(Service):
    def __init__(self, cluster: "Cluster", ctx: WorkContext, network: Network):
        super().__init__(cluster, ctx)
        self._network: Network = network
        self._node: Optional[Node] = None

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="23145d371090cfdac5715240141ad0735e1ce507b11ae2beef6d597f",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
        )

    async def start(self):
        self._node = await self._network.add_node(self.provider_id)
        self._ctx.deploy(**self._node.get_deploy_args())
        self._ctx.start()
        yield self._ctx.commit()

    async def run(self):
        ip = self._node.ip
        net = self._network.network_id
        app_key = self.cluster._engine._api_config.app_key

        print(
            f"""Connect with:
{TEXT_COLOR_CYAN}
ssh -o ProxyCommand='websocat asyncstdio: ws://127.0.0.1:7465/net-api/v1/net/{net}/tcp/{ip}/22 --binary -H=Authorization:"Bearer {app_key}"' root@{ip}
{TEXT_COLOR_DEFAULT}"""
        )

        self._ctx.run("/bin/bash", "-c", "syslogd")
        self._ctx.run("/bin/bash", "-c", "ssh-keygen -A")
        self._ctx.run("/bin/bash", "-c", "/usr/sbin/sshd")
        self._ctx.run("/bin/bash", "-c", "sleep 3600")
        yield self._ctx.commit(timeout=timedelta(minutes=30))


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

        cluster = await golem.run_service(
            SshService, instance_params=[{"network": network} for _ in range(2)]
        )

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        while True:
            print(instances())
            try:
                await asyncio.sleep(5)
            except (KeyboardInterrupt, asyncio.CancelledError):
                print("shutting down")
                break


if __name__ == "__main__":
    parser = build_parser("Golem VPN SSH example")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"ssh-yapapi-{now}.log")
    args = parser.parse_args()

    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    enable_default_logger(
        log_file=args.log_file,
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
        debug_net_api=True,
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        main(subnet_tag=args.subnet_tag, driver=args.driver, network=args.network)
    )

    try:
        loop.run_until_complete(task)
    except NoPaymentAccountError as e:
        handbook_url = (
            "https://handbook.golem.network/requestor-tutorials/"
            "flash-tutorial-of-requestor-development"
        )
        print(
            f"{TEXT_COLOR_RED}"
            f"No payment account initialized for driver `{e.required_driver}` "
            f"and network `{e.required_network}`.\n\n"
            f"See {handbook_url} on how to initialize payment accounts for a requestor node."
            f"{TEXT_COLOR_DEFAULT}"
        )
    except KeyboardInterrupt:
        print(
            f"{TEXT_COLOR_YELLOW}"
            "Shutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
            f"{TEXT_COLOR_DEFAULT}"
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
