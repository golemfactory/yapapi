import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import heapq

from typing import AsyncIterator, Awaitable, Callable, List, Optional, Tuple

from yapapi.mid.market import Proposal


@dataclass(order=True)
class ScoredProposal:
    score: float
    proposal: Proposal = field(compare=False)


class SimpleScorer:
    """Re-orders proposals using a provided scoring function."""
    def __init__(
        self,
        score_proposal: Callable[[Proposal], Awaitable[float]],
        min_proposals: Optional[int] = None,
        max_wait: Optional[timedelta] = None,
    ):
        """
        :param score_proposal: Proposal-scoring function. Higher score -> better :any:`Proposal`.
        :param min_proposals: If not None, :func:`__call__` will not yield anything until
            at least that many proposals were scored (but `max_wait` overrides this)
        :param max_wait: If not None, we'll not wait for `min_proposals` longer than that.
        """
        self._score_proposal = score_proposal
        self._min_proposals = min_proposals
        self._max_wait = max_wait

        self._scored_proposals: List[ScoredProposal] = []

    async def __call__(self, proposals: AsyncIterator[Proposal]) -> AsyncIterator[Proposal]:
        """Consumes incoming proposals as fast as possible. Always yields a :any:`Proposal` with the highest score.

        :param proposals: Stream of :class:`Proposal` to be reordered.
            In fact, this could be stream of whatever, as long as this whatever matches
            the scoring function, and this whatever would be yielded (--> TODO).
        """
        self._no_more_proposals = False
        proposal_scorer_task = asyncio.get_event_loop().create_task(self._process_stream(proposals))
        try:
            async for proposal, score in self._proposals():
                print(f"Yielding proposal with score {-1 * score}")
                yield proposal
        except asyncio.CancelledError:
            proposal_scorer_task.cancel()
            self._no_more_proposals = True

    async def _proposals(self) -> AsyncIterator[Tuple[Proposal, float]]:
        await self._wait_until_ready()

        while self._scored_proposals or not self._no_more_proposals:
            try:
                scored_proposal = heapq.heappop(self._scored_proposals)
                yield scored_proposal.proposal, scored_proposal.score
            except IndexError:
                await asyncio.sleep(0.1)

    async def score_proposal(self, proposal: Proposal) -> float:
        return await self._score_proposal(proposal)

    async def _process_stream(self, proposal_stream: AsyncIterator[Proposal]) -> None:
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
