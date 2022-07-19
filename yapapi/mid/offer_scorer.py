import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import heapq

from typing import AsyncIterator, Awaitable, Callable, List, Optional, Tuple

from yapapi.mid.market import Offer


@dataclass(order=True)
class ScoredOffer:
    score: float
    offer: Offer = field(compare=False)


class SimpleScorer:
    def __init__(
        self,
        offer_stream: AsyncIterator[Offer],
        score_offer: Callable[[Offer], Awaitable[float]],
        min_offers: Optional[int] = None,
        max_wait: Optional[timedelta] = None,
    ):
        self._score_offer = score_offer
        self._min_offers = min_offers
        self._max_wait = max_wait

        self._scored_offers: List[ScoredOffer] = []
        self._offer_scorer_task = asyncio.get_event_loop().create_task(self._process_stream(offer_stream))

    async def offers(self) -> AsyncIterator[Tuple[Offer, float]]:
        await self._wait_until_ready()

        while True:
            try:
                scored_offer = heapq.heappop(self._scored_offers)
                yield scored_offer.offer, scored_offer.score * -1
            except IndexError:
                await asyncio.sleep(0.1)

    async def score_offer(self, offer: Offer) -> float:
        return await self._score_offer(offer)

    async def aclose(self) -> None:
        self._offer_scorer_task.cancel()

    async def _process_stream(self, offer_stream: AsyncIterator[Offer]):
        async for offer in offer_stream:
            score = await self.score_offer(offer)
            score = score * -1  # heap -> smallest values first -> reverse
            heapq.heappush(self._scored_offers, ScoredOffer(score, offer))

    async def _wait_until_ready(self) -> None:
        start = datetime.now()

        while True:
            if self._min_offers is None or len(self._scored_offers) >= self._min_offers:
                break
            if self._max_wait is not None and datetime.now() - start >= self._max_wait:
                break
            await asyncio.sleep(0.1)
