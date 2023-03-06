import asyncio
from datetime import datetime, timezone
from pathlib import Path
import pytest
from typing import List, cast

from goth.configuration import Override


#  `pytest-rerunfailures` and `pytest-asyncio` don't work together
#   (https://github.com/pytest-dev/pytest-rerunfailures/issues/154)
#   The same problem occurs when `flaky` is used instead of `pytest-rerunfailures`.
#   Here we have a patch that is quite ugly, but hopefully harmless.
class LoopThatIsNeverClosed(asyncio.AbstractEventLoop):
    """Just a loop, but if you try to use it after it was closed you use a fresh loop"""

    def __init__(self):
        self._loop = None

    def __getattribute__(self, name):
        if name == "_loop":
            return super().__getattribute__("_loop")
        if self._loop is None or self._loop._closed:
            self._loop = asyncio.new_event_loop()
        return getattr(self._loop, name)


@pytest.fixture
def event_loop():
    """This overrides `pytest.asyncio` fixture"""
    loop = LoopThatIsNeverClosed()
    yield loop
    loop.close()


def _project_dir() -> Path:
    package_dir = Path(__file__).parent.parent
    return package_dir.parent.resolve()


def pytest_addoption(parser):
    """Add optional parameters to pytest CLI invocations."""

    parser.addoption(
        "--config-path",
        help="Path to the `goth-config.yml` file. (default: %(default)s)",
        default=_project_dir() / "tests" / "goth_tests" / "assets" / "goth-config.yml",
    )

    parser.addoption(
        "--config-override",
        action="append",
        help="Set an override for a value specified in goth-config.yml file. \
                This argument may be used multiple times. \
                Values must follow the convention: {yaml_path}={value}, e.g.: \
                `docker-compose.build-environment.release-tag=0.6.`",
    )

    parser.addoption(
        "--ssh-verify-connection",
        action="store_true",
        help="in the `test_run_ssh.py`, peform an actual SSH connection through "
        "the exposed websocket. Requires both `ssh` and `websocket` binaries "
        "to be available in the path.",
    )


@pytest.fixture(scope="function")
def config_overrides(request) -> List[Override]:
    """Fixture parsing --config-override params passed to the test invocation.

    This fixture has "function" scope, which means that each test function will
    receive its own copy of the list of config overrides and may modify it at will,
    without affecting other test functions run in the same session.
    """

    overrides: List[str] = request.config.option.config_override or []
    return cast(List[Override], [tuple(o.split("=", 1)) for o in overrides])


@pytest.fixture(scope="function")
def single_node_override() -> Override:
    nodes = [
        {"name": "requestor", "type": "Requestor"},
        {"name": "provider-1", "type": "VM-Wasm-Provider", "use-proxy": True},
    ]
    return "nodes", nodes


@pytest.fixture
def ssh_verify_connection(request):
    return request.config.option.ssh_verify_connection


@pytest.fixture(scope="session")
def project_dir() -> Path:
    return _project_dir()


@pytest.fixture(scope="session")
def log_dir() -> Path:
    base_dir = Path("/", "tmp", "goth-tests")
    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S%z")
    log_dir = base_dir / f"goth_{date_str}"
    log_dir.mkdir(parents=True)
    return log_dir


@pytest.fixture(scope="session")
def goth_config_path(request) -> Path:
    return request.config.option.config_path


