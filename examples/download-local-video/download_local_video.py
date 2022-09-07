#!/usr/bin/env python3
import asyncio
import base64
import pathlib
import sys
from datetime import datetime, timedelta

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
        manifest_cert = open("foo_req.cert.pem", "rb").read()
        manifest_cert = base64.b64encode(manifest_cert).decode("utf-8")

        return await vm.manifest(
            manifest=manifest,
            manifest_sig=manifest_sig,
            manifest_sig_algorithm=manifest_sig_algorithm,
            manifest_cert=manifest_cert,
            min_mem_gib=0.5,
            min_storage_gib=5,
            min_cpu_threads=1,
            capabilities=["inet", "manifest-support"],
        )

    async def run(self):
        script = self._ctx.new_script()

        future_result1 = script.run("/golem/entrypoints/request.sh", "http://yacn2.dev.golem.network:8000/docker-golem_hashcat_gpu_ok-latest-deeda00637.gvmi")
        future_result2 = script.download_file("/golem/output/output.txt", "output.txt")
        yield script

        result = (await future_result1).stdout
        print(result.strip() if result else "dupa1")
        result = (await future_result2).stdout
        print(result.strip() if result else "dupa2")
        event.set()


async def main():
    async with Golem(budget=1.0, subnet_tag="raf-local") as golem:
        await golem.run_service(ApiCallService, num_instances=1)

        await event.wait()


if __name__ == "__main__":
    enable_default_logger(log_file="download_local_video.log")
    run_golem_example(main())
