import secrets

import pytest

from yapapi.contrib.service.chunk import chunks


@pytest.mark.parametrize(
    "data, chunk_limit, num_chunks_expected, pass_as_memoryview",
    (
        (secrets.token_bytes(16), 16, 1, True),
        (secrets.token_bytes(8), 16, 1, True),
        (secrets.token_bytes(17), 16, 2, True),
        (secrets.token_bytes(31), 16, 2, True),
        (secrets.token_bytes(256), 16, 16, True),
        (secrets.token_bytes(257), 16, 17, True),
        (secrets.token_bytes(16), 16, 1, False),
        (secrets.token_bytes(8), 16, 1, False),
        (secrets.token_bytes(17), 16, 2, False),
        (secrets.token_bytes(31), 16, 2, False),
        (secrets.token_bytes(256), 16, 16, False),
        (secrets.token_bytes(257), 16, 17, False),
    ),
)
def test_chunks(data, chunk_limit, num_chunks_expected, pass_as_memoryview):
    num_chunks_received = 0
    data_out = b""
    data_in = memoryview(data) if pass_as_memoryview else data
    for chunk in chunks(data_in, chunk_limit):
        data_out += chunk
        num_chunks_received += 1

    assert num_chunks_received == num_chunks_expected
    assert data_out == data
