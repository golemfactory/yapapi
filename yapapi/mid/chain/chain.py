from typing import Any, AsyncIterator, Callable


class Chain():
    """Special class for mid-level components that utilize the pipes and filters pattern.

    Sample usage::

        async def source() -> AsyncIterator[int]:
            yield 1
            yield 2

        async def int_2_str(numbers: AsyncIterator[int]) -> AsyncIterator[str]:
            async for number in numbers:
                yield str(number)

        async for x in Chain(source, int_2_str):
            # x == "1"
            # x == "2"

    More complex usages --> [TODO - examples, run.py].
    """

    def __init__(self, chain_start: AsyncIterator[Any], *pipes: Callable[[AsyncIterator[Any]], AsyncIterator[Any]]):
        aiter = chain_start

        for pipe in pipes:
            aiter = pipe(aiter)

        self._aiter = aiter

    def __aiter__(self) -> "Chain":
        return self

    async def __anext__(self) -> Any:
        return await self._aiter.__anext__()
