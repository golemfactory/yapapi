#!/usr/bin/env python3
import argparse
import asyncio
from asyncio import TimeoutError
from datetime import datetime, timezone
import json
import logging

from yapapi import enable_default_logger
from yapapi import props as yp
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market, Activity, Payment  # noqa


async def list_offers(conf: Configuration, subnet_tag="testnet"):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.Identification(name="some scannig node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.props, dbuild.cons) as subscription:
            async for event in subscription.events():
                print(f"Offer: {event.id}")
                print(f"from {event.issuer}")
                print(f"props {json.dumps(event.props, indent=4)}")
                print("\n\n")
        print("done")


def build_parser():
    parser = argparse.ArgumentParser(description="Render blender scene")
    parser.add_argument("--subnet-tag", default="testnet")
    parser.add_argument(
        "--debug", dest="log_level", action="store_const", const=logging.DEBUG, default=logging.INFO
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    enable_default_logger(level=args.log_level)
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(list_offers(Configuration(), subnet_tag=args.subnet_tag,), timeout=4,)
        )
    except TimeoutError:
        pass


if __name__ == "__main__":
    main()
