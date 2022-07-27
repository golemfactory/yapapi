from typing import AsyncIterator, Optional

from yapapi.mid.market import Agreement, Proposal


class AgreementCreator:
    #   TODO: this class doesn't really make much sense (e.g. because it negotiates only a single
    #         agreement at a given time) - we'd rather have an AgreementPool instead
    async def __call__(self, proposals: AsyncIterator[Proposal]) -> AsyncIterator[Agreement]:
        async for proposal in proposals:
            agreement = await self._create_agreement(proposal)
            if agreement is not None:
                yield agreement

    async def _create_agreement(self, proposal: Proposal) -> Optional[Agreement]:
        agreement = await proposal.create_agreement()
        await agreement.confirm()

        approved = await agreement.wait_for_approval()
        if approved:
            return agreement
        return None
