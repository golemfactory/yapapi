import asyncio
from typing import AsyncIterator, List, Optional

from yapapi.mid.market import Offer


class DummyNegotiator:
    def __init__(self, buffor_size: int = 1):
        self.buffor_size = buffor_size

        self.main_task: Optional[asyncio.Task] = None
        self.tasks: List[asyncio.Task] = []
        self.queue: asyncio.Queue[Offer] = asyncio.Queue()

    async def __call__(self, offers: AsyncIterator[Offer]) -> AsyncIterator[Offer]:
        self._no_more_offers = False
        self.main_task = asyncio.create_task(self._process_offers(offers))

        while not self.queue.empty() or not self._no_more_offers:
            yield await self.queue.get()

    async def _process_offers(self, offers: AsyncIterator[Offer]) -> None:
        semaphore = asyncio.Semaphore(self.buffor_size)

        async for offer in offers:
            await semaphore.acquire()
            self.tasks.append(asyncio.create_task(self._negotiate_offer(offer, semaphore)))

        self._no_more_offers = True
        self._main_task = None

    async def _negotiate_offer(self, offer: Offer, semaphore: asyncio.Semaphore) -> None:
        try:
            proposal = await self._get_proposal(offer)
            if proposal is not None:
                self.queue.put_nowait(proposal)
        finally:
            semaphore.release()

    async def _get_proposal(self, offer: Offer) -> Optional[Offer]:
        our_response = await offer.respond()
        print(f"Responded to {offer} with {our_response}")

        async for their_response in our_response.responses():
            print(f"They responded with {their_response} to {our_response}")
            return their_response
        print(f"Our response {our_response} was rejected")
        return None
