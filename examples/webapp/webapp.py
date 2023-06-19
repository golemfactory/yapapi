#!/usr/bin/env python3
import asyncio
import json
import pathlib
import sys
from datetime import datetime, timedelta
from decimal import Decimal

from yapapi import Golem
from yapapi.contrib.service.http_proxy import HttpProxyService, LocalHttpProxy
from yapapi.network import Network
from yapapi.payload import vm
from yapapi.props import com
from yapapi.services import Service, ServiceState
from yapapi.strategy import (
    DecreaseScoreForUnconfirmedAgreement,
    LeastExpensiveLinearPayuMS,
    PROP_DEBIT_NOTE_INTERVAL_SEC,
    PropValueRange,
)

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


DEBIT_NOTE_INTERVAL_SEC = 3600


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

        script = self._ctx.new_script(timeout=timedelta(seconds=20))

        script.run(
            "/bin/bash",
            "-c",
            f"cd /webapp && python app.py "
            f"--db-address {self._db_address} "
            f"--db-port {self._db_port}"
            f" initdb",
        )
        script.run(
            "/bin/bash",
            "-c",
            f"cd /webapp && python app.py "
            f"--db-address {self._db_address} "
            f"--db-port {self._db_port} "
            f"run > /webapp/out 2> /webapp/err &",
        )
        yield script

    def _serialize_init_params(self):
        return {"db_address": self._db_address, "db_port": self._db_port, }


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


class MyMarketStrategy(LeastExpensiveLinearPayuMS):
    acceptable_prop_value_range_overrides = {
        PROP_DEBIT_NOTE_INTERVAL_SEC: PropValueRange(DEBIT_NOTE_INTERVAL_SEC, None),
    }


async def main(subnet_tag, payment_driver, payment_network, port):

    base_strategy = MyMarketStrategy(
        max_fixed_price=Decimal("1.0"),
        max_price_for={com.Counter.CPU: Decimal("0.2"), com.Counter.TIME: Decimal("0.1")},
    )
    strategy = DecreaseScoreForUnconfirmedAgreement(base_strategy, 0.5)

    golem = Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
        strategy=strategy,
    )

    golem.add_event_consumer(strategy.on_event)

    await golem.start()

    print_env_info(golem)

    network = await golem.create_network("192.168.0.1/24")
    db_cluster = await golem.run_service(DbService, network=network)
    db_instance = db_cluster.instances[0]

    def still_starting(cluster):
        return any(
            i.state in (ServiceState.pending, ServiceState.starting) for i in cluster.instances
        )

    def raise_exception_if_still_starting(cluster):
        if still_starting(cluster):
            raise Exception(
                f"Failed to start {cluster} instances "
                f"after {STARTING_TIMEOUT.total_seconds()} seconds"
            )

    commissioning_time = datetime.now()

    while still_starting(db_cluster) and datetime.now() < commissioning_time + STARTING_TIMEOUT:
        print(db_cluster.instances)
        await asyncio.sleep(5)

    raise_exception_if_still_starting(db_cluster)

    print(f"{TEXT_COLOR_CYAN}DB instance started, spawning the web server{TEXT_COLOR_DEFAULT}")

    web_cluster = await golem.run_service(
        HttpService,
        network=network,
        instance_params=[{"db_address": db_instance.network_node.ip}],
    )

    # wait until all remote http instances are started

    while still_starting(web_cluster) and datetime.now() < commissioning_time + STARTING_TIMEOUT:
        print(web_cluster.instances + db_cluster.instances)
        await asyncio.sleep(5)

    raise_exception_if_still_starting(web_cluster)

    # service instances started, start the local HTTP server

    proxy = LocalHttpProxy(web_cluster, port)
    await proxy.run()

    print(
        f"{TEXT_COLOR_CYAN}Local HTTP server listening on:\n"
        f"http://localhost:{port}{TEXT_COLOR_DEFAULT}"
    )

    secs = 7
    print(f"{TEXT_COLOR_CYAN}waiting {secs} seconds...{TEXT_COLOR_DEFAULT}")
    await asyncio.sleep(secs)

    await proxy.stop()
    print(f"{TEXT_COLOR_CYAN}HTTP server stopped{TEXT_COLOR_DEFAULT}")

    print("=================================================================== SERIALIZING AND DROPPING CURRENT STATE")

    network_serialized = network.serialize()
    db_serialized = db_cluster.serialize_instances()
    web_serialized = web_cluster.serialize_instances()

    web_cluster.suspend()
    db_cluster.suspend()

    print(f"{TEXT_COLOR_CYAN}waiting {secs} seconds...{TEXT_COLOR_DEFAULT}")
    await asyncio.sleep(secs)

    print("=================================================================== STOPPING GOLEM ENGINE")

    await golem.stop(wait_for_payments=False)

    print(f"{TEXT_COLOR_CYAN}waiting {secs} seconds...{TEXT_COLOR_DEFAULT}")
    await asyncio.sleep(secs)


    print("=================================================================== SERIALIZED STATE: ")

    print(json.dumps([network_serialized, db_serialized, web_serialized], indent=4))

    print(f"{TEXT_COLOR_CYAN}waiting {secs} seconds...{TEXT_COLOR_DEFAULT}")
    await asyncio.sleep(secs)

    print("=================================================================== RESTARTING THE ENGINE AND THE SERVICES")

    golem = Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    )

    await golem.start()

    print_env_info(golem)


    network = Network.deserialize(golem._engine._net_api, network_serialized)

    db_cluster = await golem.resume_service(DbService, instances=db_serialized, network=network)
    web_cluster = await golem.resume_service(HttpService, instances=web_serialized, network=network)

    print([i.state for i in web_cluster.instances])

    raise_exception_if_still_starting(web_cluster)

    proxy = LocalHttpProxy(web_cluster, port)
    await proxy.run()

    print(
        f"{TEXT_COLOR_CYAN}Local HTTP server listening on:\n"
        f"http://localhost:{port}{TEXT_COLOR_DEFAULT}"
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

    await network.remove()
    await golem.stop()


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
