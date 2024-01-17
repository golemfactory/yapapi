import asyncio
import base64
import io
import json
from PIL import Image
import requests

from dataclasses import dataclass
from datetime import datetime

from yapapi import Golem
from yapapi.payload import Payload
from yapapi.props import inf
from yapapi.props.base import constraint, prop
from yapapi.services import Service
from yapapi.log import enable_default_logger
from yapapi.config import ApiConfig

import argparse
import asyncio
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import time
import colorama  # type: ignore

from yapapi import Golem, NoPaymentAccountError
from yapapi import __version__ as yapapi_version
from yapapi import windows_event_loop_fix
from yapapi.log import enable_default_logger
from yapapi.strategy import SCORE_TRUSTED, SCORE_REJECTED, MarketStrategy
from yapapi.rest import Activity

from ya_activity import ApiClient, ApiException, RequestorControlApi, RequestorStateApi

# Utils

TEXT_COLOR_RED = "\033[31;1m"
TEXT_COLOR_GREEN = "\033[32;1m"
TEXT_COLOR_YELLOW = "\033[33;1m"
TEXT_COLOR_BLUE = "\033[34;1m"
TEXT_COLOR_MAGENTA = "\033[35;1m"
TEXT_COLOR_CYAN = "\033[36;1m"
TEXT_COLOR_WHITE = "\033[37;1m"

TEXT_COLOR_DEFAULT = "\033[0m"

colorama.init()


def build_parser(description: str) -> argparse.ArgumentParser:
    current_time_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S%z")
    default_log_path = Path(tempfile.gettempdir()) / f"yapapi_{current_time_str}.log"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--payment-driver", "--driver", help="Payment driver name, for example `erc20`"
    )
    parser.add_argument(
        "--payment-network", "--network", help="Payment network name, for example `goerli`"
    )
    parser.add_argument("--subnet-tag", help="Subnet name, for example `public`")
    parser.add_argument(
        "--log-file",
        default=str(default_log_path),
        help="Log file for YAPAPI; default: %(default)s",
    )
    return parser


def format_usage(usage):
    return {
        "current_usage": usage.current_usage,
        "timestamp": usage.timestamp.isoformat(sep=" ") if usage.timestamp else None,
    }


def print_env_info(golem: Golem):
    print(
        f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
        f"Using subnet: {TEXT_COLOR_YELLOW}{golem.subnet_tag}{TEXT_COLOR_DEFAULT}, "
        f"payment driver: {TEXT_COLOR_YELLOW}{golem.payment_driver}{TEXT_COLOR_DEFAULT}, "
        f"and network: {TEXT_COLOR_YELLOW}{golem.payment_network}{TEXT_COLOR_DEFAULT}\n"
    )


def run_golem_example(example_main, log_file=None):
    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    if log_file:
        enable_default_logger(
            log_file=log_file,
            debug_activity_api=True,
            debug_market_api=True,
            debug_payment_api=True,
            debug_net_api=True,
        )

    loop = asyncio.get_event_loop()
    task = loop.create_task(example_main)

    try:
        loop.run_until_complete(task)
    except NoPaymentAccountError as e:
        handbook_url = (
            "https://handbook.golem.network/requestor-tutorials/"
            "flash-tutorial-of-requestor-development"
        )
        print(
            f"{TEXT_COLOR_RED}"
            f"No payment account initialized for driver `{e.required_driver}` "
            f"and network `{e.required_network}`.\n\n"
            f"See {handbook_url} on how to initialize payment accounts for a requestor node."
            f"{TEXT_COLOR_DEFAULT}"
        )
    except KeyboardInterrupt:
        print(
            f"{TEXT_COLOR_YELLOW}"
            "Shutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
            f"{TEXT_COLOR_DEFAULT}"
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass


class ProviderOnceStrategy(MarketStrategy):
    """Hires provider only once.
    """

    def __init__(self):
        self.history = set(())

    async def score_offer(self, offer):
        if offer.issuer not in self.history:
            return SCORE_TRUSTED
        else:
            return SCORE_REJECTED


    def remember(self, provider_id: str):
        self.history.add(provider_id)

# App

RUNTIME_NAME = "automatic"
CAPABILITIES = "golem.runtime.capabilities"

@dataclass
class AiPayload(Payload):
    image_url: str = prop("golem.!exp.ai.v1.srv.comp.ai.model")
    image_fmt: str = prop("golem.!exp.ai.v1.srv.comp.ai.model-format", default="safetensors")

    runtime: str = constraint(inf.INF_RUNTIME_NAME, default=RUNTIME_NAME)
    capabilities: str = constraint(CAPABILITIES, default="automatic")


class AiRuntimeService(Service):
    @staticmethod
    async def get_payload():
        # return AiPayload(image_url="hash:sha3:92180a67d096be309c5e6a7146d89aac4ef900e2bf48a52ea569df7d:https://huggingface.co/stabilityai/stable-diffusion-xl-base-1.0/resolve/main/sd_xl_base_1.0.safetensors?download=true")
        return AiPayload(image_url="hash:sha3:6ce0161689b3853acaa03779ec93eafe75a02f4ced659bee03f50797806fa2fa:https://huggingface.co/runwayml/stable-diffusion-v1-5/resolve/main/v1-5-pruned-emaonly.safetensors?download=true")
    async def start(self):
        self.strategy.remember(self._ctx.provider_id)

        script = self._ctx.new_script(timeout=None)
        script.deploy()
        script.start(
        )
        yield script

    # async def run(self):
    #    # TODO run AI tasks here

    def __init__(self, strategy: ProviderOnceStrategy):
        super().__init__()
        self.strategy = strategy

async def trigger(activity: RequestorControlApi, token, prompt, output_file):

    #TODO Wait for the Automatic server to start.
    time.sleep(30)

    custom_url = "/sdapi/v1/txt2img"
    url = activity._api.api_client.configuration.host + f"/activity/{activity.id}/proxy_http_request" + custom_url
    headers = {"Authorization": "Bearer "+token}

    payload = {
        'prompt': prompt,
        'steps': 5
    }
    
    response = requests.post(url, headers=headers, json=payload, stream=True)
    if response.encoding is None:
        response.encoding = 'utf-8'

    for line in response.iter_lines(decode_unicode=True):
        if line:
            # print(line)
            response = json.loads(line)
            image = Image.open(io.BytesIO(base64.b64decode(response['images'][0])))
            image.save(output_file)


async def main(subnet_tag, driver=None, network=None):
    strategy = ProviderOnceStrategy()
    async with Golem(
        budget=50.0,
        subnet_tag=subnet_tag,
        strategy=strategy,
        payment_driver=driver,
        payment_network=network,
    ) as golem:
        cluster = await golem.run_service(
            AiRuntimeService,
            instance_params=[
                {"strategy": strategy}
            ],
            num_instances=1,
        )

        async def get_image(prompt, file_name):
            for s in cluster.instances:
                if s._ctx != None:
                    for id in [s._ctx._activity.id ]:
                        activity = await golem._engine._activity_api.use_activity(id)
                        await trigger(
                            activity,
                            golem._engine._api_config.app_key,
                            prompt,
                            file_name
                        )

        print('Starting')
        print('Please input your prompt:')
        prompt = input()
        await asyncio.sleep(10)
        await get_image(
            prompt,
            'output.png'
        )
        print('Done')

if __name__ == "__main__":
    parser = build_parser("Run AI runtime task")
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"ai-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            driver=args.payment_driver,
            network=args.payment_network,
        ),
        log_file=args.log_file,
    )
