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


# @pytest.mark.asyncio
# async def test_double_iteration():
#
#     class TestIterable(AsyncIterable[int]):
#
#         def __aiter__(self):
#             async def iterable():
#                 for item in [1, 2, 3]:
#                     yield item
#             return iterable()
#
#     test = TestIterable()
#
#     peekable = PeekableAsyncIterator(test)
#
#     assert await peekable.has_items()
#
#     async for item in peekable:
#         assert item == 1
#         break
#
#     assert await peekable.has_items()
#
#     async for item in peekable:
#         assert item == 2
#         break
#
#     assert await peekable.has_items()
#
#     async for item in peekable:
#         assert item == 3
#         break
#
#     assert not await peekable.has_items()
#
