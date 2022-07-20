import asyncio
from typing import AsyncIterator, List, Optional

from yapapi.mid.market import Agreement, Offer


class AgreementPool:
    def __init__(self, size: int = 1):
        self.size = size

        self.main_task: Optional[asyncio.Task] = None
        self.tasks: List[asyncio.Task] = []
        self.agreements: List[Agreement] = []
    
    async def __call__(self, offers: AsyncIterator[Offer]) -> AsyncIterator[Agreement]:
        async for offer in offers:
            agreement = await self._create_agreement(offer)
            yield agreement

    # async def __call__(self, offers: AsyncIterator[Offer]) -> AsyncIterator[Agreement]:
    #     self._no_more_offers = False
    #     self.main_task = asyncio.create_task(self._process_offers(offers))

    #     while self.agreements or not self._no_more_offers:
    #         try:
    #             agreement = next(a for a in self.agreements if a.activity_possible)
    #         except StopIteration:
    #             await asyncio.sleep(0.1)
    #         else:
    #             yield agreement

    # async def _process_offers(self, offers: AsyncIterator[Offer]) -> None:
    #     while True:
    #         running_tasks = [t for t in self.tasks if not t.done()]
    #         if len(running_tasks) + len(self.agreements) < self.size:
    #             try:
    #                 offer = await offers.__anext__()
    #             except StopAsyncIteration:
    #                 break
    #             self.tasks.append(asyncio.create_task(self._create_agreement(offer)))
    #     self._no_more_offers = True

    # async def _create_agreement(self, offer: Offer) -> None:
    #     print("CREATE AGREEMENT START!", offer)
    #     agreement = await offer.create_agreement()
    #     print("CREATED", agreement)
    #     await agreement.confirm()
    #     print("CONFIRMED", agreement)
    #     await agreement.wait_for_approval()
    #     print("APPROVED", agreement)
    #     self.agreements.append(agreement)
    
    async def _create_agreement(self, offer: Offer) -> None:
        print("CREATE AGREEMENT START!", offer)
        agreement = await offer.create_agreement()
        print("CREATED", agreement)
        await agreement.confirm()
        print("CONFIRMED", agreement)
        await agreement.wait_for_approval()
        print("APPROVED", agreement)
        return agreement
