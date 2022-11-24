from typing import Generator, TypeVar

BufferType = TypeVar("BufferType", bytes, memoryview)


def chunks(data: BufferType, chunk_limit: int) -> Generator[BufferType, None, None]:
    """Split the input buffer into chunks of `chunk_limit` bytes."""
    max_chunk, remainder = divmod(len(data), chunk_limit)
    for chunk in range(0, max_chunk + (1 if remainder else 0)):
        yield data[chunk * chunk_limit : (chunk + 1) * chunk_limit]
