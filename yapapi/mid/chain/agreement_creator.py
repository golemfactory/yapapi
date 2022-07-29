from typing import AsyncIterator, Optional

from yapapi.mid.market import Agreement, Proposal


class AgreementCreator:
    """Uses a :any:`Proposal` to create an :any:`Agreement`, confirms it and waits for approval."""
    #   TODO: this class doesn't really make much sense (e.g. because it negotiates only a single
    #         agreement at a given time) - we'd rather have an AgreementPool instead
    async def __call__(self, proposals: AsyncIterator[Proposal]) -> AsyncIterator[Agreement]:
        """Consumes :any:`Proposal`. Yields confirmed & approved :any:`Agreement`.

        At most a single :any:`Agreement` is being created at any given time, so that's pretty
        inefficient.

        :param proposals: A stream of :any:`Proposal` (that should be in a state that allows to
            :any:`Proposal.create_agreement` on them).
        """
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
