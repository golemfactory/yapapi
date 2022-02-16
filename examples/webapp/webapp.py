#!/usr/bin/env python3
import asyncio
from datetime import timedelta
import pathlib
import random
import sys
import string
from uuid import uuid4


from datetime import datetime

from yapapi import (
    Golem,
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
    print_env_info,
)

SSH_RQLITE_CLIENT_IMAGE_HASH = "1fa641433cb2c7eb0f88d87e92c32ca01755e46c0b922dfb285dfcbf"
RQLITE_IMAGE_HASH = "85021afecf51687ecae8bdc21e10f3b11b82d2e3b169ba44e177340c"


class ClientService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash=SSH_RQLITE_CLIENT_IMAGE_HASH,
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def start(self):
        # perform the initialization of the Service
        # (which includes sending the network details within the `deploy` command)
        async for script in super().start():
            yield script

        password = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))

        script = self._ctx.new_script(timeout=timedelta(seconds=10))
        script.run("/bin/bash", "-c", "syslogd")
        script.run("/bin/bash", "-c", "ssh-keygen -A")
        script.run("/bin/bash", "-c", f'echo -e "{password}\n{password}" | passwd')
        script.run("/bin/bash", "-c", "/usr/sbin/sshd")
        yield script

        connection_uri = self.network_node.get_websocket_uri(22)
        app_key = self.cluster.service_runner._job.engine._api_config.app_key

        print(
            "Connect with:\n"
            f"{TEXT_COLOR_CYAN}"
            f"ssh -o ProxyCommand='websocat asyncstdio: {connection_uri} --binary -H=Authorization:\"Bearer {app_key}\"' root@{uuid4().hex}"
            f"{TEXT_COLOR_DEFAULT}"
        )

        print(f"{TEXT_COLOR_RED}password: {password}{TEXT_COLOR_DEFAULT}")

    async def reset(self):
        # We don't have to do anything when the service is restarted
        pass


class RqliteService(Service):
    def __init__(self):
        super().__init__()

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash=RQLITE_IMAGE_HASH,
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def start(self):
        # perform the initialization of the Service
        # (which includes sending the network details within the `deploy` command)
        async for script in super().start():
            yield script

        script = self._ctx.new_script(timeout=timedelta(seconds=30))
        script.run("/bin/run_rqlite.sh")
        yield script

    async def reset(self):
        # We don't have to do anything when the service is restarted
        pass


async def main(subnet_tag, payment_driver=None, payment_network=None):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        network = await golem.create_network("192.168.0.1/24")
        async with network:
            client_cluster = await golem.run_service(
                ClientService, network=network, num_instances=1
            )

            rql_cluster = await golem.run_service(RqliteService, network=network, num_instances=1)

            while True:
                print(client_cluster.instances)
                print(rql_cluster.instances)
                try:
                    await asyncio.sleep(5)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    break

            client_cluster.stop()
            rql_cluster.stop()

            cnt = 0
            while cnt < 3 and any(
                s.is_available for s in client_cluster.instances + rql_cluster.instances
            ):
                print(client_cluster.instances)
                print(rql_cluster.instances)
                await asyncio.sleep(5)
                cnt += 1


if __name__ == "__main__":
    parser = build_parser("Golem simple Web app example")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"webapp-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        ),
        log_file=args.log_file,
    )
