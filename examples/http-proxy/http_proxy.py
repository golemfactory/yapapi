#!/usr/bin/env python3
"""
a simple http proxy example
"""
import asyncio

from datetime import datetime, timedelta, timezone
import pathlib
import shlex
import sys


from yapapi import Golem
from yapapi.services import ServiceState
from yapapi.payload import vm

from yapapi.contrib.service.http_proxy import HttpProxyService, LocalHttpProxy


examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    run_golem_example,
    print_env_info,
)

# the timeout after we commission our service instances
# before we abort this script
STARTING_TIMEOUT = timedelta(minutes=4)

# additional expiration margin to allow providers to take our offer,
# as providers typically won't take offers that expire sooner than 5 minutes in the future
EXPIRATION_MARGIN = timedelta(minutes=5)


class HttpService(HttpProxyService):
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
        script = self._ctx.new_script(timeout=timedelta(minutes=1))
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

    async def reset(self):
        # We don't have to do anything when the service is restarted
        pass


# ######## Main application code which spawns the Golem service and the local HTTP server


async def main(subnet_tag, payment_driver, payment_network, num_instances, port, running_time):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        commissioning_time = datetime.now()

        network = await golem.create_network("192.168.0.1/24")
        cluster = await golem.run_service(
            HttpService,
            network=network,
            num_instances=num_instances,
            expiration=datetime.now(timezone.utc)
            + STARTING_TIMEOUT
            + EXPIRATION_MARGIN
            + timedelta(seconds=running_time),
        )
        instances = cluster.instances

        def still_starting():
            return any(i.state in (ServiceState.pending, ServiceState.starting) for i in instances)

        # wait until all remote http instances are started

        while still_starting() and datetime.now() < commissioning_time + STARTING_TIMEOUT:
            print(f"instances: {instances}")
            await asyncio.sleep(5)

        if still_starting():
            raise Exception(
                f"Failed to start instances after {STARTING_TIMEOUT.total_seconds()} seconds"
            )

        # service instances started, start the local HTTP server

        proxy = LocalHttpProxy(cluster, port)
        await proxy.run()

        print(
            f"{TEXT_COLOR_CYAN}Local HTTP server listening on:\nhttp://localhost:{port}{TEXT_COLOR_DEFAULT}"
        )

        # wait until Ctrl-C

        start_time = datetime.now()

        while datetime.now() < start_time + timedelta(seconds=running_time):
            print(instances)
            try:
                await asyncio.sleep(10)
            except (KeyboardInterrupt, asyncio.CancelledError):
                break

        # perform the shutdown of the local http server and the service cluster

        await proxy.stop()
        print(f"{TEXT_COLOR_CYAN}HTTP server stopped{TEXT_COLOR_DEFAULT}")

        cluster.stop()

        cnt = 0
        while cnt < 3 and any(s.is_available for s in instances):
            print(instances)
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
    parser.add_argument(
        "--running-time",
        default=600,
        type=int,
        help=(
            "How long should the instance run before the cluster is stopped "
            "(in seconds, default: %(default)s)"
        ),
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
            running_time=args.running_time,
        ),
        log_file=args.log_file,
    )
