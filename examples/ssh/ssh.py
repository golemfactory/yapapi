#!/usr/bin/env python3
import asyncio
import pathlib
import random
import sys
import string
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
    print_env_info,
)


class SshService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="1e06505997e8bd1b9e1a00bd10d255fc6a390905e4d6840a22a79902",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
            # we're adding an additional constraint to only select those nodes that
            # are offering VPN-capable VM runtimes so that we can connect them to the VPN
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def run(self):
        connection_uri = self.network_node.get_websocket_uri(22)
        app_key = self.cluster._engine._api_config.app_key

        password = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))

        script = self._ctx.new_script()
        script.run("/bin/bash", "-c", "syslogd")
        script.run("/bin/bash", "-c", "ssh-keygen -A")
        script.run("/bin/bash", "-c", f'echo -e "{password}\n{password}" | passwd')
        script.run("/bin/bash", "-c", "/usr/sbin/sshd")
        yield script

        print(
            "Connect with:\n"
            f"{TEXT_COLOR_CYAN}"
            f"ssh -o ProxyCommand='websocat asyncstdio: {connection_uri} --binary -H=Authorization:\"Bearer {app_key}\"' root@{uuid4().hex}"
            f"{TEXT_COLOR_DEFAULT}"
        )

        print(f"{TEXT_COLOR_RED}password: {password}{TEXT_COLOR_DEFAULT}")

        # await indefinitely...
        await asyncio.Future()


async def main(subnet_tag, payment_driver=None, payment_network=None):
    # By passing `event_consumer=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        network = await golem.create_network("192.168.0.1/24")
        async with network:
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
            while cnt < 3 and any(s.is_available for s in cluster.instances):
                print(instances())
                await asyncio.sleep(5)
                cnt += 1


if __name__ == "__main__":
    parser = build_parser("Golem VPN SSH example")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"ssh-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        ),
        log_file=args.log_file,
    )
