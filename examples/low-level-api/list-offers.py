from yapapi.rest import Configuration, Market, Activity, Payment  # noqa
from yapapi import props as yp
from yapapi.props.builder import DemandBuilder
from datetime import datetime, timezone
import json
import asyncio
from asyncio import TimeoutError


async def list_offers(conf: Configuration):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.Identification(name="some scannig node", subnet_tag="testnet"))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.props, dbuild.cons) as subscription:
            async for event in subscription.events():
                print(f"Offer: {event.id}")
                print(f"from {event.issuer}")
                print(f"props {json.dumps(event.props, indent=4)}")
                print("\n\n")
        print("done")


try:
    asyncio.get_event_loop().run_until_complete(
        asyncio.wait_for(list_offers(Configuration()), timeout=4)
    )
except TimeoutError:
    pass
