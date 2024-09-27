#!/usr/bin/env python3
import asyncio
from asyncio import TimeoutError
from datetime import datetime, timezone
import sys
import pathlib
from typing import AsyncIterator, List

from yapapi import props as yp
from yapapi.log import enable_default_logger
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market, Activity, Payment  # noqa
from yapapi.rest.market import OfferProposal
from examples import utils

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))


class NodeInfo(object):
    def __init__(self, offer: OfferProposal):
        self.node_id = offer.issuer
        self.node_name = offer.props["golem.node.id.name"]


async def list_offers(conf: Configuration, subnet_tag: str) -> AsyncIterator[OfferProposal]:
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="Scanning Node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():
                yield event


async def list_nodes(conf: Configuration, subnet_tag: str) -> AsyncIterator[List[NodeInfo]]:
    nodes = set()
    async for offer in list_offers(conf, subnet_tag):
        node = NodeInfo(offer)

        def equal(node_in_set: NodeInfo):
            return node.node_id == node_in_set.node_id

        if not any([equal(n) for n in nodes]):
            nodes.add(node)
            yield node


async def print_nodes(conf: Configuration, subnet_tag: str):
    async for node in list_nodes(conf, subnet_tag=subnet_tag):
        print(f"{node.node_id}    {node.node_name}")


def main():
    parser = utils.build_parser("List Nodes")
    args = parser.parse_args()

    if args.subnet_tag is None:
        subnet = "devnet-beta"
    else:
        subnet = args.subnet_tag

    sys.stderr.write(f"Using subnet: {utils.TEXT_COLOR_YELLOW}{subnet}{utils.TEXT_COLOR_DEFAULT}\n")

    enable_default_logger()
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                print_nodes(
                    Configuration(),
                    subnet_tag=subnet,
                ),
                timeout=10,
            )
        )
    except TimeoutError:
        pass


if __name__ == "__main__":
    main()
