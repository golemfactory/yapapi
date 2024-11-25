#!/usr/bin/env python3

import asyncio
import os
import pathlib
import sys
from datetime import datetime
from typing import List

from alive_progress import alive_bar
from dataclasses import dataclass
from utils import build_parser, run_golem_example

import yapapi.script.command
from yapapi import Golem
from yapapi.payload import Payload, vm
from yapapi.payload.vm import VmPackageFormat, VmRequest, _VmPackage
from yapapi.props import inf
from yapapi.props.base import constraint
from yapapi.props.builder import DemandBuilder
from yapapi.script import ProgressArgs
from yapapi.services import Service

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))


def command_key(event: "yapapi.events.CommandProgress") -> str:
    return f"{event.script_id}#{event.command._index}"


class ProgressDisplayer:
    def __init__(self):
        self._transfers_bars = {}
        self._transfers_ctx = {}

    def exit(self):
        for key, bar in self._transfers_ctx:
            bar.__exit__(None, None, None)

    def progress_bar(self, event: "yapapi.events.CommandProgress"):
        if event.message is not None:
            print(f"{event.message}")

        if event.progress is not None and event.progress[1] is not None:
            progress = event.progress
            key = command_key(event)

            if self._transfers_ctx.get(key) is None:
                self.create_progress_bar(event)

            bar = self._transfers_ctx.get(key)
            bar(progress[0] / progress[1])

    def create_progress_bar(self, event: "yapapi.events.CommandProgress"):
        key = command_key(event)
        bar = alive_bar(
            total=event.progress[1],
            manual=True,
            title="Progress",
            unit=event.unit,
            scale=True,
            dual_line=True,
        )
        bar_ctx = bar.__enter__()

        command = event.command
        if isinstance(command, yapapi.script.command.Deploy):
            bar_ctx.text = "Deploying image"
        elif isinstance(command, yapapi.script.command._SendContent):
            bar_ctx.text = f"Uploading file: {command._src.download_url} -> {command._dst_path}"
        elif isinstance(command, yapapi.script.command._ReceiveContent):
            bar_ctx.text = f"Downloading file: {command._src_path} -> {command._dst_path}"

        self._transfers_bars[key] = bar
        self._transfers_ctx[key] = bar_ctx

    def executed(self, event: "yapapi.events.CommandExecuted"):
        key = command_key(event)
        if self._transfers_ctx.get(key) is not None:
            bar_obj = self._transfers_bars.get(key)
            bar = self._transfers_ctx.get(key)

            bar(1.0)
            bar_obj.__exit__(None, None, None)

            self._transfers_bars.pop(key)
            self._transfers_ctx.pop(key)


@dataclass
class ExamplePayload(Payload):
    image_url: str
    min_mem_gib: float = constraint(inf.INF_MEM, operator=">=")
    min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=")
    min_cpu_threads: int = constraint(inf.INF_THREADS, operator=">=")

    capabilities: List[vm.VmCaps] = constraint(
        "golem.runtime.capabilities", operator="=", default_factory=list
    )

    runtime: str = constraint(inf.INF_RUNTIME_NAME, operator="=", default=vm.RUNTIME_VM)

    # Constraints can't be bool, because python serializes bool to `True` and market matcher
    # expects `true`.
    deploy_progress_capability: str = constraint(
        "golem.activity.caps.deploy.report-progress", operator="=", default="true"
    )
    transfer_progress_capability: str = constraint(
        "golem.activity.caps.transfer.report-progress", operator="=", default="true"
    )

    async def decorate_demand(self, demand: DemandBuilder):
        await super().decorate_demand(demand)
        demand.add(
            VmRequest(package_url=self.image_url, package_format=VmPackageFormat.GVMKIT_SQUASH)
        )


class ExampleService(Service):
    @staticmethod
    async def get_payload():
        package: _VmPackage = await vm.repo(
            image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
            min_mem_gib=0.5,
            min_storage_gib=10.0,
        )
        return ExamplePayload(
            image_url=package.image_url,
            min_mem_gib=package.constraints.min_mem_gib,
            min_storage_gib=package.constraints.min_storage_gib,
            min_cpu_threads=package.constraints.min_cpu_threads,
            capabilities=package.constraints.capabilities,
            runtime=package.constraints.runtime,
        )

    async def start(self):
        script = self._ctx.new_script(timeout=None)
        script.deploy(progress_args=ProgressArgs(updateInterval="300ms"))
        script.start()

        yield script

    async def run(self):
        progress = ProgressArgs(updateInterval="300ms")

        script = self._ctx.new_script(timeout=None)
        script.download_from_url(
            "https://huggingface.co/cointegrated/rubert-tiny2/resolve/main/model.safetensors",
            "/golem/resource/model-small",
            progress_args=progress,
        )
        script.upload_bytes(
            os.urandom(40 * 1024 * 1024), "/golem/resource/bytes.bin", progress_args=progress
        )
        script.download_file("/golem/resource/bytes.bin", "download.bin", progress_args=progress)
        yield script

        os.remove("download.bin")
        await self.cluster.terminate()


shutdown = False


async def main(subnet_tag, driver=None, network=None):
    async with Golem(
        budget=50.0,
        subnet_tag=subnet_tag,
        payment_driver=driver,
        payment_network=network,
        stream_output=True,
    ) as golem:
        global shutdown

        bar = ProgressDisplayer()
        await golem.run_service(
            ExampleService,
            num_instances=1,
        )

        def progress_event_handler(event: "yapapi.events.CommandProgress"):
            bar.progress_bar(event)

        def on_shutdown(_event: "yapapi.events.ServiceFinished"):
            global shutdown
            bar.exit()
            shutdown = True

        def on_command_executed(event: "yapapi.events.CommandExecuted"):
            bar.executed(event)

        golem.add_event_consumer(progress_event_handler, ["CommandProgress"])
        golem.add_event_consumer(on_shutdown, ["ServiceFinished"])
        golem.add_event_consumer(on_command_executed, ["CommandExecuted"])

        while not shutdown:
            await asyncio.sleep(1)


if __name__ == "__main__":
    parser = build_parser("Run transfer progress example app")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"progress-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            driver=args.payment_driver,
            network=args.payment_network,
        ),
        log_file=args.log_file,
    )
