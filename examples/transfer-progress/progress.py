#!/usr/bin/env python3

import pathlib
import sys
import json
from dataclasses import dataclass
from alive_progress import alive_bar

from yapapi.payload import Payload, vm
from yapapi.props import inf
from yapapi.props.base import constraint, prop
from yapapi.script import ProgressArgs
from yapapi.services import Service

import asyncio
from datetime import datetime

import colorama  # type: ignore

from yapapi import Golem

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_MAGENTA,
    TEXT_COLOR_YELLOW,
    build_parser,
    format_usage,
    print_env_info,
    run_golem_example,
)

RUNTIME_NAME = "ai"
CAPABILITIES = "golem.runtime.capabilities"


class ProgressDisplayer:
    def __init__(self):
        self._transfers_bars = {}
        self._transfers_ctx = {}

    def exit(self):
        for key, bar in self._transfers_ctx:
            bar.__exit__(None, None, None)

    def progress_bar(self, event: "yapapi.events.CommandProgress"):
        if event.progress is not None:
            progress = event.progress

            if progress[1] is not None:
                if self._transfers_ctx.get(event.script_id) is None:
                    bar = alive_bar(total=progress[1], manual=True, title="Progress", unit=event.unit, scale=True,
                                    dual_line=True)
                    bar_ctx = bar.__enter__()
                    bar_ctx.text = f"Uploading file: {event.command._src_url} -> {event.command._dst_path}"

                    self._transfers_bars[event.script_id] = bar
                    self._transfers_ctx[event.script_id] = bar_ctx

                bar = self._transfers_ctx.get(event.script_id)
                bar(progress[0] / progress[1])

    def executed(self, event: "yapapi.events.CommandExecuted"):
        if self._transfers_ctx.get(event.script_id) is not None:
            bar_obj = self._transfers_bars.get(event.script_id)
            bar = self._transfers_ctx.get(event.script_id)

            bar(1.0)
            bar_obj.__exit__(None, None, None)

            self._transfers_bars.pop(event.script_id)
            self._transfers_ctx.pop(event.script_id)


@dataclass
class ExamplePayload(Payload):
    image_url: str = prop("golem.!exp.ai.v1.srv.comp.ai.model")
    image_fmt: str = prop("golem.!exp.ai.v1.srv.comp.ai.model-format", default="safetensors")

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)
    capabilities: str = constraint(CAPABILITIES, default="dummy")


class ExampleService(Service):
    # @staticmethod
    # async def get_payload():
    #     return ExamplePayload(image_url="hash:sha3:0b682cf78786b04dc108ff0b254db1511ef820105129ad021d2e123a7b975e7c:https://huggingface.co/cointegrated/rubert-tiny2/resolve/main/model.safetensors?download=true")

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
        )

    async def start(self):
        async for script in super().start():
            yield script

    async def run(self):
        progress = ProgressArgs()

        script = self._ctx.new_script(timeout=None)
        script.upload_from_url(
            "https://huggingface.co/cointegrated/rubert-tiny2/resolve/main/model.safetensors?download=true",
            "/golem/resource/model-small", progress_args=progress)
        yield script

        script = self._ctx.new_script(timeout=None)
        script.upload_from_url(
            "https://registry.golem.network/v1/image/download?tag=golem/ray-on-golem:0.6.1-py3.10.13-ray2.7.1&https=true",
            "/golem/resource/model-big", progress_args=progress)
        yield script


async def main(subnet_tag, driver=None, network=None):
    async with Golem(
            budget=50.0,
            subnet_tag=subnet_tag,
            payment_driver=driver,
            payment_network=network,
            stream_output=True,
    ) as golem:
        bar = ProgressDisplayer()
        cluster = await golem.run_service(
            ExampleService,
            num_instances=1,
        )

        def progress_event_handler(event: "yapapi.events.CommandProgress"):
            bar.progress_bar(event)

        def on_shutdown(_event: "yapapi.events.ServiceFinished"):
            bar.exit()

        def on_command_executed(event: "yapapi.events.CommandExecuted"):
            bar.executed(event)

        golem.add_event_consumer(progress_event_handler, ["CommandProgress"])
        golem.add_event_consumer(on_shutdown, ["ServiceFinished"])
        golem.add_event_consumer(on_command_executed, ["CommandExecuted"])

        while True:
            await asyncio.sleep(8)


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
