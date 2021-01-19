from dns.exception import DNSException
import pytest
from srvresolver.srv_resolver import SRVRecord  # type: ignore
from unittest import mock

from yapapi.package import Package, PackageException
from yapapi.package.vm import resolve_repo_srv, _FALLBACK_REPO_URL

_MOCK_HOST = 'non.existent.domain'
_MOCK_PORT = 9999


@mock.patch(
    "yapapi.package.vm.SRVResolver.resolve_random",
    mock.Mock(return_value=SRVRecord(host=_MOCK_HOST, port=_MOCK_PORT, weight=1, priority=1))
)
def test_resolve_srv():
    assert resolve_repo_srv("") == f"http://{_MOCK_HOST}:{_MOCK_PORT}"


@mock.patch(
    "yapapi.package.vm.SRVResolver.resolve_random",
    mock.Mock(side_effect=DNSException())
)
def test_resolve_srv_exception():
    # should be:

    # with pytest.raises(DNSException):
    #     assert resolve_repo_srv("")

    # temporary work-around:
    assert resolve_repo_srv("") == _FALLBACK_REPO_URL
