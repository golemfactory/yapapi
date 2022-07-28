from typing import Any, AsyncIterator, Callable


class Chain():
    def __init__(self, chain_start: AsyncIterator[Any], *pipes: Callable[[AsyncIterator[Any]], AsyncIterator[Any]]):
        aiter = chain_start

        for pipe in pipes:
            aiter = pipe(aiter)

        self._aiter = aiter

    def __aiter__(self) -> "Chain":
        return self

    async def __anext__(self) -> Any:
        return await self._aiter.__anext__()
