import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import heapq

from typing import AsyncIterator, Awaitable, Callable, List, Optional

from yapapi.mid.market import Proposal


@dataclass(order=True)
class ScoredProposal:
    score: float
    proposal: Proposal = field(compare=False)


class SimpleScorer:
    def __init__(
        self,
        score_proposal: Callable[[Proposal], Awaitable[float]],
        min_proposals: Optional[int] = None,
        max_wait: Optional[timedelta] = None,
    ):
        self._score_proposal = score_proposal
        self._min_proposals = min_proposals
        self._max_wait = max_wait

        self._scored_proposals: List[ScoredProposal] = []

    async def __call__(self, proposals: AsyncIterator[Proposal]) -> AsyncIterator[Proposal]:
        self._no_more_proposals = False
        proposal_scorer_task = asyncio.get_event_loop().create_task(self._process_stream(proposals))
        try:
            async for proposal, score in self._proposals():
                print(f"Yielding proposal with score {-1 * score}")
                yield proposal
        except asyncio.CancelledError:
            proposal_scorer_task.cancel()
            self._no_more_proposals = True

    async def _proposals(self):
        await self._wait_until_ready()

        while self._scored_proposals or not self._no_more_proposals:
            try:
                scored_proposal = heapq.heappop(self._scored_proposals)
                yield scored_proposal.proposal, scored_proposal.score
            except IndexError:
                await asyncio.sleep(0.1)

    async def score_proposal(self, proposal: Proposal) -> float:
        return await self._score_proposal(proposal)

    async def _process_stream(self, proposal_stream: AsyncIterator[Proposal]):
        async for proposal in proposal_stream:
            score = await self.score_proposal(proposal)
            score = score * -1  # heap -> smallest values first -> reverse
            heapq.heappush(self._scored_proposals, ScoredProposal(score, proposal))
        self._no_more_proposals = True

    async def _wait_until_ready(self) -> None:
        start = datetime.now()

        while True:
            if self._min_proposals is None or len(self._scored_proposals) >= self._min_proposals:
                break
            if self._max_wait is not None and datetime.now() - start >= self._max_wait:
                break
            await asyncio.sleep(0.1)
