from collections.abc import AsyncIterator


class Chain(AsyncIterator):
    def __init__(self, *chain_parts):
        aiter = chain_parts[0]

        for pipe in chain_parts[1:]:
            aiter = pipe(aiter)

        self._aiter = aiter

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._aiter.__anext__()
