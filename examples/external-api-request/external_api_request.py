#!/usr/bin/env python3
import asyncio
import base64
from datetime import datetime
import pathlib
import sys

from yapapi import Golem
from yapapi.services import Service
from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))


from utils import (
    build_parser,
    run_golem_example,
    print_env_info,
)

class ApiCallService(Service):
    @staticmethod
    async def get_payload():
        manifest = open("manifest.json", "rb").read()
        manifest = base64.b64encode(manifest).decode("utf-8")

        manifest_sig = open("manifest.json.base64.sign.sha256", "rb").read()
        manifest_sig = base64.b64encode(manifest_sig).decode("utf-8")

        manifest_sig_algorithm = "sha256"

        # both DER and PEM formats are supported
        manifest_cert = open("requestor.cert.der", "rb").read()
        manifest_cert = base64.b64encode(manifest_cert).decode("utf-8")

        return await vm.manifest(
            manifest=manifest,
            manifest_sig=manifest_sig,
            manifest_sig_algorithm=manifest_sig_algorithm,
            manifest_cert=manifest_cert,
            min_mem_gib=0.5,
            min_cpu_threads=0.5,
            capabilities=["inet", "manifest-support"],
        )

    async def run(self):
        script = self._ctx.new_script()
        future_result = script.run(
            "/bin/sh",
            "-c",
            f"GOLEM_PRICE=`curl -X 'GET' \
                    'https://api.coingecko.com/api/v3/simple/price?ids=golem&vs_currencies=usd' \
                    -H 'accept: application/json' | jq .golem.usd`; \
                echo ---; \
                echo \"Golem price: $GOLEM_PRICE USD\"; \
                echo ---;",
        )
        yield script

        result = (await future_result).stdout
        print(result.strip() if result else "")
        service_finished_event.set()

async def main(subnet_tag, payment_driver, payment_network):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        cluster = await golem.run_service(ApiCallService, num_instances=1)

        while True:
            print(cluster.instances)
            try:
                await asyncio.sleep(10)
            except (KeyboardInterrupt, asyncio.CancelledError):
                break

if __name__ == "__main__":
    parser = build_parser("External API request example")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"external-api-request-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        ),
        log_file=args.log_file,
    )
