from typing import AsyncIterator, Optional

from yapapi.mid.market import Agreement, Offer


class AgreementCreator:
    #   TODO: this class doesn't really make much sense (e.g. because it negotiates only a single
    #         agreement at a given time) - we'd rather have an AgreementPool instead
    async def __call__(self, offers: AsyncIterator[Offer]) -> AsyncIterator[Agreement]:
        async for offer in offers:
            agreement = await self._create_agreement(offer)
            if agreement is not None:
                yield agreement

    async def _create_agreement(self, offer: Offer) -> Optional[Agreement]:
        agreement = await offer.create_agreement()
        await agreement.confirm()

        approved = await agreement.wait_for_approval()
        if approved:
            return agreement
        return None
