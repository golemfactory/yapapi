#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta
import pathlib
import sys

from yapapi import Golem
from yapapi.contrib.service.http_proxy import HttpProxyService, LocalHttpProxy
from yapapi.payload import vm
from yapapi.services import Service, ServiceState

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    build_parser,
    print_env_info,
    run_golem_example,
)

HTTP_IMAGE_HASH = "c37c1364f637c199fe710ca62241ff486db92c875b786814c6030aa1"
DB_IMAGE_HASH = "85021afecf51687ecae8bdc21e10f3b11b82d2e3b169ba44e177340c"

STARTING_TIMEOUT = timedelta(minutes=4)


class HttpService(HttpProxyService):
    def __init__(self, db_address: str, db_port: int = 4001):
        super().__init__(remote_port=5000)
        self._db_address = db_address
        self._db_port = db_port

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash=HTTP_IMAGE_HASH,
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def start(self):
        # perform the initialization of the Service
        # (which includes sending the network details within the `deploy` command)
        async for script in super().start():
            yield script

        script = self._ctx.new_script()

        script.run(
            "/bin/bash",
            "-c",
            f"cd /webapp && python app.py --db-address {self._db_address} --db-port {self._db_port} initdb",
        )
        script.run(
            "/bin/bash",
            "-c",
            f"cd /webapp && python app.py --db-address {self._db_address} --db-port {self._db_port} run > /webapp/out 2> /webapp/err &",
        )
        yield script


class DbService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash=DB_IMAGE_HASH,
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


async def main(subnet_tag, payment_driver, payment_network, port):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        network = await golem.create_network("192.168.0.1/24")
        async with network:
            db_cluster = await golem.run_service(DbService, network=network)
            db_instance = db_cluster.instances[0]

            def still_starting(cluster):
                return any(
                    i.state in (ServiceState.pending, ServiceState.starting)
                    for i in cluster.instances
                )

            def raise_exception_if_still_starting(cluster):
                if still_starting(cluster):
                    raise Exception(
                        f"Failed to start {cluster} instances "
                        f"after {STARTING_TIMEOUT.total_seconds()} seconds"
                    )

            commissioning_time = datetime.now()

            while (
                still_starting(db_cluster)
                and datetime.now() < commissioning_time + STARTING_TIMEOUT
            ):
                print(db_cluster.instances)
                await asyncio.sleep(5)

            raise_exception_if_still_starting(db_cluster)

            print(
                f"{TEXT_COLOR_CYAN}DB instance started, spawning the web server{TEXT_COLOR_DEFAULT}"
            )

            web_cluster = await golem.run_service(
                HttpService,
                network=network,
                instance_params=[{"db_address": db_instance.network_node.ip}],
            )

            # wait until all remote http instances are started

            while (
                still_starting(web_cluster)
                and datetime.now() < commissioning_time + STARTING_TIMEOUT
            ):
                print(web_cluster.instances + db_cluster.instances)
                await asyncio.sleep(5)

            raise_exception_if_still_starting(web_cluster)

            # service instances started, start the local HTTP server

            proxy = LocalHttpProxy(web_cluster, port)
            await proxy.run()

            print(
                f"{TEXT_COLOR_CYAN}Local HTTP server listening on:\nhttp://localhost:{port}{TEXT_COLOR_DEFAULT}"
            )

            # wait until Ctrl-C

            while True:
                print(web_cluster.instances + db_cluster.instances)
                try:
                    await asyncio.sleep(10)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    break

            # perform the shutdown of the local http server and the service cluster

            await proxy.stop()
            print(f"{TEXT_COLOR_CYAN}HTTP server stopped{TEXT_COLOR_DEFAULT}")

            web_cluster.stop()
            db_cluster.stop()

            cnt = 0
            while cnt < 3 and any(
                s.is_available for s in web_cluster.instances + db_cluster.instances
            ):
                print(web_cluster.instances + db_cluster.instances)
                await asyncio.sleep(5)
                cnt += 1


if __name__ == "__main__":
    parser = build_parser("Golem simple Web app example")
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="The local port to listen on",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"webapp-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            port=args.port,
        ),
        log_file=args.log_file,
    )
