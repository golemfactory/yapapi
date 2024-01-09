#!/usr/bin/env python3

import pathlib
import sys
import json
from dataclasses import dataclass

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


def progress_event_handler(event: "yapapi.events.CommandProgress"):
    if event.progress is not None:
        progress = event.progress

        percent = 0.0
        if progress[1] is None:
            percent = "unknown"
        else:
            percent = 100.0 * progress[0] / progress[1]

        print(f"Transfer progress: {percent}% ({progress[0]} {event['unit']} / {progress[1]} {event['unit']})")


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
        script = self._ctx.new_script(timeout=None)

        progress = ProgressArgs()
        script.upload_from_url(
            "hash:sha3:92180a67d096be309c5e6a7146d89aac4ef900e2bf48a52ea569df7d:https://huggingface.co/stabilityai/stable-diffusion-xl-base-1.0/resolve/main/sd_xl_base_1.0.safetensors?download=true",
            "/golem/resource/model-big", progress_args=progress)
        script.upload_from_url(
            "hash:sha3:0b682cf78786b04dc108ff0b254db1511ef820105129ad021d2e123a7b975e7c:https://huggingface.co/cointegrated/rubert-tiny2/resolve/main/model.safetensors?download=true",
            "/golem/resource/model-small", progress_args=progress)

        yield script


async def main(subnet_tag, driver=None, network=None):
    async with Golem(
            budget=50.0,
            subnet_tag=subnet_tag,
            payment_driver=driver,
            payment_network=network,
            stream_output=True,
    ) as golem:
        cluster = await golem.run_service(
            ExampleService,
            num_instances=1,
        )

        golem.add_event_consumer(progress_event_handler, ["CommandProgress"])

        def instances():
            return [f"{s.provider_name}: {s.state.value}" for s in cluster.instances]

        while True:
            await asyncio.sleep(3)
            print(f"instances: {instances()}")


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
