import pytest

from yapapi.storage import Content, Destination


class _TestDestination(Destination):
    def __init__(self, test_data: bytes):
        self._test_data = test_data

    def upload_url(self):
        return ""

    async def download_stream(self) -> Content:
        async def data():
            for c in [self._test_data]:
                yield self._test_data

        return Content(len(self._test_data), data())


@pytest.mark.asyncio
async def test_download_bytes():
    expected = b"some test data"
    destination = _TestDestination(expected)
    assert await destination.download_bytes() == expected


@pytest.mark.asyncio
async def test_download_bytes_with_limit():
    destination = _TestDestination(b"some test data")
    assert await destination.download_bytes(limit=4) == b"some"
