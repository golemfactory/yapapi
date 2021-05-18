from typing import AsyncIterable

import pytest

from yapapi.executor._smartq import PeekableAsyncIterator


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "input",
    [
        [],
        [1],
        [1, 2],
        [1, 2, 3],
        [1, 2, 3, 4],
    ],
)
async def test_iterator(input):
    async def iterator():
        for item in input:
            yield item

    it = PeekableAsyncIterator(iterator())
    assert (await it.has_next()) == bool(input)

    output = []

    async for item in it:
        output.append(item)
        assert (await it.has_next()) == (len(output) < len(input))

    assert not await it.has_next()
    assert input == output
