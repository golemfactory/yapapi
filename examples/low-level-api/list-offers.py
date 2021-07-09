#!/usr/bin/env python3
import asyncio
from asyncio import TimeoutError
from datetime import datetime, timezone
import json
import sys
import pathlib

from yapapi import props as yp
from yapapi.log import enable_default_logger
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market, Activity, Payment  # noqa

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))
import utils



async def list_offers(conf: Configuration, subnet_tag: str):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="some scanning node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():
                print(f"Offer: {event.id}")
                print(f"from {event.issuer}")
                print(f"props {json.dumps(event.props, indent=4)}")
                print("\n\n")
        print("done")


def main():
    parser = utils.build_parser("List offers")
    args = parser.parse_args()

    subnet = args.subnet_tag
    sys.stderr.write(f"Using subnet: {utils.TEXT_COLOR_YELLOW}{subnet}{utils.TEXT_COLOR_DEFAULT}\n")

    enable_default_logger()
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag=subnet,
                ),
                timeout=4,
            )
        )
    except TimeoutError:
        pass


if __name__ == "__main__":
    main()
