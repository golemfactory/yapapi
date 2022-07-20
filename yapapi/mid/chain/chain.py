from typing import Any, AsyncIterator


class Chain:
    def __init__(self, *args):
        self.chain_parts = args

    async def __aiter__(self) -> AsyncIterator[Any]:
        iterator = self.chain_parts[0]

        for pipe in self.chain_parts[1:]:
            iterator = pipe(iterator)

        async for x in iterator:
            yield x
