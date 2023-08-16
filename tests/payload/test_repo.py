from unittest.mock import AsyncMock

import pytest

from yapapi.payload import vm
from yapapi.payload.package import PackageException

_MOCK_HTTP_ADDR = "http://test.address/"
_MOCK_HTTPS_ADDR = "https://test.address/"
_MOCK_SHA3 = "abcdef124356789"
_MOCK_SIZE = 2**24


async def _mock_response(*args, **kwargs):
    mock = AsyncMock()
    mock.status = 200
    mock.json.return_value = {
        "http": _MOCK_HTTP_ADDR,
        "https": _MOCK_HTTPS_ADDR,
        "sha3": _MOCK_SHA3,
        "size": _MOCK_SIZE,
    }
    return mock


@pytest.mark.parametrize(
    "image_hash, image_tag, image_url, image_use_https, "
    "expected_url, expected_error, expected_error_msg",
    (
        ("testhash", None, None, False, f"hash:sha3:{_MOCK_SHA3}:{_MOCK_HTTP_ADDR}", None, ""),
        (None, "testtag", None, False, f"hash:sha3:{_MOCK_SHA3}:{_MOCK_HTTP_ADDR}", None, ""),
        ("testhash", None, None, True, f"hash:sha3:{_MOCK_SHA3}:{_MOCK_HTTPS_ADDR}", None, ""),
        (None, "testtag", None, True, f"hash:sha3:{_MOCK_SHA3}:{_MOCK_HTTPS_ADDR}", None, ""),
        ("testhash", None, "http://image", False, "hash:sha3:testhash:http://image", None, ""),
        (
            None,
            None,
            None,
            False,
            None,
            PackageException,
            "Either an image_hash or an image_tag is required "
            "to resolve an image URL from the Golem Registry",
        ),
        (
            None,
            None,
            "http://image",
            False,
            None,
            ValueError,
            "An image_hash is required when using a direct image_url",
        ),
        (
            None,
            "testtag",
            "http://image",
            False,
            None,
            ValueError,
            "An image_tag can only be used when resolving "
            "from Golem Registry, not with a direct image_url",
        ),
        (
            "testhash",
            "testtag",
            None,
            False,
            None,
            PackageException,
            "Golem Registry images can be resolved by either "
            "an image_hash or by an image_tag but not both",
        ),
    ),
)
@pytest.mark.asyncio
async def test_repo(
    monkeypatch,
    image_hash,
    image_tag,
    image_url,
    image_use_https,
    expected_url,
    expected_error,
    expected_error_msg,
):
    monkeypatch.setattr("aiohttp.ClientSession.get", _mock_response)
    monkeypatch.setattr("aiohttp.ClientSession.head", _mock_response)

    package_awaitable = vm.repo(
        image_hash=image_hash,
        image_tag=image_tag,
        image_url=image_url,
        image_use_https=image_use_https,
    )

    if expected_error:
        with pytest.raises(expected_error) as e:
            _ = await package_awaitable
        assert expected_error_msg in str(e)
    else:
        package = await package_awaitable
        url = await package.resolve_url()
        assert url == expected_url
