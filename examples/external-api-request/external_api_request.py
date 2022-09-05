#!/usr/bin/env python3
import asyncio
import base64
from datetime import timedelta
import pathlib
import sys

from yapapi import Golem
from yapapi.services import Service
from yapapi.log import enable_default_logger
from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import run_golem_example

event = asyncio.Event()


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
        event.set()


async def main():
    async with Golem(budget=1.0, subnet_tag="devnet-beta") as golem:
        await golem.run_service(ApiCallService, num_instances=1)
        await event.wait()


if __name__ == "__main__":
    enable_default_logger(log_file="external_api_request.log")
    run_golem_example(main())
