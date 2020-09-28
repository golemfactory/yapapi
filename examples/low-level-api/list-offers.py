#!/usr/bin/env python3
import asyncio
from asyncio import TimeoutError
from datetime import datetime, timezone
import json

from yapapi import enable_default_logger
from yapapi import props as yp
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market, Activity, Payment  # noqa


async def list_offers(conf: Configuration, subnet_tag="testnet"):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.Identification(name="some scanning node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.props, dbuild.cons) as subscription:
            async for event in subscription.events():
                print(f"Offer: {event.id}")
                print(f"from {event.issuer}")
                print(f"props {json.dumps(event.props, indent=4)}")
                print("\n\n")
        print("done")


def main():
    import pathlib
    import sys

    parent_directory = pathlib.Path(__file__).resolve().parent.parent
    sys.stderr.write(f"Adding {parent_directory} to sys.path.\n")
    sys.path.append(str(parent_directory))
    import utils

    parser = utils.build_parser("List offers")
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
