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

from utils import (run_golem_example, TEXT_COLOR_CYAN, TEXT_COLOR_DEFAULT, TEXT_COLOR_RED)

task_finished_event = asyncio.Event()

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
        url = "http://speedtest.tele2.net/1GB.zip"

        request_result = script.run("/golem/entrypoints/request.sh", url)
        script.download_file("/golem/output/output.txt", "output.txt")
        yield script

        result = (await request_result).stdout
    
        if result:
            print(
                f"{TEXT_COLOR_CYAN}"
                f"Golem Network took: {result.strip()} seconds to download a file from {url}"
                f"{TEXT_COLOR_DEFAULT}")
        else:
            print(
                f"{TEXT_COLOR_RED}"
                f"Error: Did not compute"
                f"{TEXT_COLOR_DEFAULT}")

        task_finished_event.set()


async def main():
    async with Golem(budget=1.0, subnet_tag="raf-local") as golem:
        await golem.run_service(ApiCallService, num_instances=1)

        await task_finished_event.wait()


if __name__ == "__main__":
    enable_default_logger(log_file="download_local_video.log")
    run_golem_example(main())
