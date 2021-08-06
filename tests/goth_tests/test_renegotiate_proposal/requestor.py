#!/usr/bin/env python3
import asyncio
from asyncio import TimeoutError
import datetime
import sys
from typing_extensions import Final
import ya_market
from yapapi import props as yp
from yapapi.log import enable_default_logger
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market
from yapapi.rest.market import OfferProposal

from examples import utils

DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP: Final[str] = "golem.com.payment.debit-notes.accept-timeout?"
PROPOSALS_LIMIT: Final[int] = 4


async def _respond(proposal: OfferProposal, dbuild) -> str:
    dbuild.properties["golem.com.payment.chosen-platform"] = "NGNT"
    timeout = proposal.props.get(DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP)
    dbuild.properties[DEBIT_NOTE_ACCEPTANCE_TIMEOUT_PROP] = timeout
    return await proposal.respond(dbuild.properties, dbuild.constraints)


async def renegotiate_offers(conf: Configuration, subnet_tag: str):
    """Rejects every proposal & then renegotiates it"""
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="some renegotiating node", subnet_tag=subnet_tag))
        dbuild.add(
            yp.Activity(
                expiration=datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=30)
            )
        )

        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            issuers = set()
            proposals = 0
            rejected_proposals = set()  # Already rejected, don't reject again
            async for event in subscription.events():
                node_name = event.props.get("golem.node.id.name")
                proposal_id = event._proposal.proposal.proposal_id
                print(f"\n[{node_name}] {'*'*15} {proposal_id}")
                prev_proposal_id = event._proposal.proposal.prev_proposal_id
                print(f"[{node_name}] prev_proposal_id: {prev_proposal_id}")
                if not event.is_draft:
                    if proposals > PROPOSALS_LIMIT:
                        print(f"[node_name] Skipping additional proposal")
                        break
                    await _respond(event, dbuild)
                    proposals += 1
                    issuers.add(event.issuer)
                    print(f"[{node_name}] Responded. proposals={proposals}, issuers={len(issuers)}")
                    continue

                print(
                    f"[{node_name}] Offer: {proposal_id} from {event.issuer} is_draft: {event.is_draft}"
                )
                if prev_proposal_id not in rejected_proposals:
                    await event.reject()
                    print(f"[{node_name}] Rejected {len(rejected_proposals)}. id: {proposal_id}")
                    await asyncio.sleep(1)
                    print(f"[{node_name}] Renegotiating. id: {proposal_id}")
                    new_offer_id = await _respond(event, dbuild)
                    print(f"[{node_name}] new_offer_id: {new_offer_id}")
                    rejected_proposals.add(new_offer_id)
                    continue
                print(".create_agreement()")
                agreement = await event.create_agreement()
                print(".confirm()")
                confirm_result = await agreement.confirm()
                print(f"[{node_name}] agreement.confirm(): {confirm_result}")
                if confirm_result:
                    terminate_reason = {
                        "message": "Work cancelled",
                        "golem.requestor.code": "Cancelled",
                    }
                    terminate_result = await agreement.terminate(terminate_reason)
                    print(f"agreement.terminate(): {terminate_result}")
        print("All done")


def main():
    subnet = "goth"

    enable_default_logger()
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                renegotiate_offers(
                    Configuration(),
                    subnet_tag=subnet,
                ),
                timeout=140,
            )
        )
    except TimeoutError:
        print("Main timeout triggered :(")


if __name__ == "__main__":
    main()
