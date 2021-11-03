import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import cast, List

import pytest

from goth.configuration import Override
from goth.runner.container.compose import ComposeNetworkManager
from goth.runner.process import run_command
from yapapi.package import vm


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


@pytest.fixture(autouse=True)
def docker_compose_down_remove_orphans(monkeypatch):
    """Add --remove-orphans, hoping this will end flaky CI errors like
    https://github.com/golemfactory/yapapi/runs/3672786561

    If this helps in `yapapi`, we'll consider putting this inside goth"""

    async def stop_network(self, compose_containers=None):
        for name, monitor in self._log_monitors.items():
            await monitor.stop()

        self._disconnect_containers(compose_containers or [])

        await run_command(
            [
                "docker-compose",
                "-f",
                str(self.config.file_path),
                "down",
                "--remove-orphans",
                "-t",
                "0",
            ]
        )

    monkeypatch.setattr(ComposeNetworkManager, "stop_network", stop_network)


def pytest_addoption(parser):
    """Add optional parameters to pytest CLI invocations."""

    parser.addoption(
        "--config-override",
        action="append",
        help="Set an override for a value specified in goth-config.yml file. \
                This argument may be used multiple times. \
                Values must follow the convention: {yaml_path}={value}, e.g.: \
                `docker-compose.build-environment.release-tag=0.6.`",
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


@pytest.fixture(scope="session")
def project_dir() -> Path:
    package_dir = Path(__file__).parent.parent
    return package_dir.parent.resolve()


@pytest.fixture(scope="session")
def log_dir() -> Path:
    base_dir = Path("/", "tmp", "goth-tests")
    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S%z")
    log_dir = base_dir / f"goth_{date_str}"
    log_dir.mkdir(parents=True)
    return log_dir


@pytest.fixture(scope="session")
def goth_config_path(project_dir) -> Path:
    return project_dir / "tests" / "goth_tests" / "assets" / "goth-config.yml"


@pytest.fixture()
def blender_vm_package():
    async def coro():
        return await vm.repo(
            image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
        )

    return coro()
