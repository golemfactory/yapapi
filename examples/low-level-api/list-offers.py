#!/usr/bin/env python3
import asyncio
import json
import pathlib
import sys
from asyncio import TimeoutError
from datetime import datetime, timezone

from yapapi import props as yp
from yapapi.config import ApiConfig
from yapapi.log import enable_default_logger
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Activity, Configuration, Market, Payment  # noqa

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))
import utils


async def list_offers(conf: Configuration, subnet_tag: str):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="some scanning node", subnet_tag="public"))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))
        # dbuild.ensure("(golem.node.net.is-public=true)")
        # dbuild.ensure("(golem.runtime.name=vm)")
        # dbuild.ensure("(golem.com.payment.platform.erc20-goerli-tglm.address=*)")

        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():

                props = {k: v for k, v in event.props.items() if k in ['golem.com.usage.vector', "golem.runtime.name"]}
                # props = event.props
                print(f"{json.dumps(props)}")
        print("done")


def main():
    parser = utils.build_parser("List offers")
    args = parser.parse_args()

    subnet = args.subnet_tag
    sys.stderr.write(f"Using subnet: {utils.TEXT_COLOR_YELLOW}{subnet}{utils.TEXT_COLOR_DEFAULT}\n")

    enable_default_logger()
    try:
        asyncio.run(
            asyncio.wait_for(
                list_offers(
                    Configuration(api_config=ApiConfig()),  # YAGNA_APPKEY will be loaded from env
                    subnet_tag=subnet,
                ),
                timeout=600,
            )
        )
    except TimeoutError:
        pass


if __name__ == "__main__":
    main()
