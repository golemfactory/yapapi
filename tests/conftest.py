import pytest

from yapapi.config import ApiConfig
from yapapi.golem import Golem


def pytest_addoption(parser):

    parser.addoption("--ya-api-key", type=str, help="instance api key", dest="yaApiKey")


@pytest.fixture
def dummy_yagna_engine(monkeypatch):
    """Use this fixture to call `_Engine.start()` in unit tests, without yagna/gftp.

    So also e.g. `async with Golem(..., APP_KEY='FAKE_APP_KEY')`, or Golem.start().

    But first check if monkeypatches done here don't interefere with
    the thing you want to test ofc."""
    from yapapi.engine import _Engine
    from yapapi.storage.gftp import GftpProvider

    async def _engine_create_allocations(self):
        pass

    async def _gftp_aenter(self):
        return self

    async def _gftp_aexit(self, *args):
        return None

    monkeypatch.setattr(_Engine, "_create_allocations", _engine_create_allocations)
    monkeypatch.setattr(GftpProvider, "__aenter__", _gftp_aenter)
    monkeypatch.setattr(GftpProvider, "__aexit__", _gftp_aexit)


@pytest.fixture
def api_config_factory():
    def _api_config_factory(**kwargs) -> ApiConfig:
        if "app_key" not in kwargs:
            kwargs["app_key"] = "yagna-app-key"
        return ApiConfig(**kwargs)

    return _api_config_factory


@pytest.fixture
def golem_factory(api_config_factory) -> Golem:
    def _golem_factory(**kwargs) -> Golem:
        if "api_config" not in kwargs:
            kwargs["api_config"] = api_config_factory()
        return Golem(**kwargs)

    return _golem_factory
