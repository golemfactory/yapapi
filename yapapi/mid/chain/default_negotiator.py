import asyncio
from typing import AsyncIterator, List, Optional

from yapapi.mid.market import Proposal


class DefaultNegotiator:
    def __init__(self, buffer_size: int = 1):
        self.main_task: Optional[asyncio.Task] = None
        self.tasks: List[asyncio.Task] = []
        self.queue: asyncio.Queue[Proposal] = asyncio.Queue()

        #   Acquire to start negotiations. Release:
        #   A) after failed negotiations
        #   B) when yielding proposal
        #   --> we always have (current_negotiations + proposals_ready) == buffer_size
        self.semaphore = asyncio.BoundedSemaphore(buffer_size)

    async def __call__(self, proposals: AsyncIterator[Proposal]) -> AsyncIterator[Proposal]:
        self._no_more_proposals = False
        self.main_task = asyncio.create_task(self._process_proposals(proposals))

        while not self.queue.empty() or not self._no_more_proposals or self._has_running_tasks:
            proposal = await self.queue.get()
            self.semaphore.release()
            yield proposal

    @property
    def _has_running_tasks(self) -> bool:
        return not all(task.done() for task in self.tasks)

    async def _process_proposals(self, proposals: AsyncIterator[Proposal]) -> None:
        async for proposal in proposals:
            await self.semaphore.acquire()
            self.tasks.append(asyncio.create_task(self._negotiate_proposal(proposal)))

        self._no_more_proposals = True

    async def _negotiate_proposal(self, proposal: Proposal) -> None:
        try:
            final_proposal = await self._get_proposal(proposal)
            if final_proposal is not None:
                self.queue.put_nowait(final_proposal)
            else:
                self.semaphore.release()
        except Exception:
            self.semaphore.release()

    async def _get_proposal(self, proposal: Proposal) -> Optional[Proposal]:
        our_response = await proposal.respond()
        print(f"Responded to {proposal} with {our_response}")

        async for their_response in our_response.responses():
            print(f"They responded with {their_response} to {our_response}")
            return their_response
        print(f"Our response {our_response} was rejected")
        return None
