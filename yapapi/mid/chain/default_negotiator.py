import asyncio
from typing import AsyncIterator, List, Optional

from yapapi.mid.market import Offer


class DefaultNegotiator:
    def __init__(self, buffer_size: int = 1):
        self.main_task: Optional[asyncio.Task] = None
        self.tasks: List[asyncio.Task] = []
        self.queue: asyncio.Queue[Offer] = asyncio.Queue()

        #   Acquire to start negotiations. Release:
        #   A) after failed negotiations
        #   B) when yielding offer
        #   --> we always have (current_negotiations + offers_ready) == buffer_size
        self.semaphore = asyncio.BoundedSemaphore(buffer_size)

    async def __call__(self, offers: AsyncIterator[Offer]) -> AsyncIterator[Offer]:
        self._no_more_offers = False
        self.main_task = asyncio.create_task(self._process_offers(offers))

        while not self.queue.empty() or not self._no_more_offers or self._has_running_tasks:
            proposal = await self.queue.get()
            self.semaphore.release()
            yield proposal

    @property
    def _has_running_tasks(self) -> bool:
        return not all(task.done() for task in self.tasks)

    async def _process_offers(self, offers: AsyncIterator[Offer]) -> None:
        async for offer in offers:
            await self.semaphore.acquire()
            self.tasks.append(asyncio.create_task(self._negotiate_offer(offer)))

        self._no_more_offers = True

    async def _negotiate_offer(self, offer: Offer) -> None:
        try:
            proposal = await self._get_proposal(offer)
            if proposal is not None:
                self.queue.put_nowait(proposal)
            else:
                self.semaphore.release()
        except Exception:
            self.semaphore.release()

    async def _get_proposal(self, offer: Offer) -> Optional[Offer]:
        our_response = await offer.respond()
        print(f"Responded to {offer} with {our_response}")

        async for their_response in our_response.responses():
            print(f"They responded with {their_response} to {our_response}")
            return their_response
        print(f"Our response {our_response} was rejected")
        return None
