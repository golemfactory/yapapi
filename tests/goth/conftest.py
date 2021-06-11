from datetime import datetime, timezone
from pathlib import Path
from typing import cast, List, Tuple

import pytest

from goth.configuration import Override
from yapapi.package import Package, vm


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


@pytest.fixture()
def log_dir() -> Path:
    base_dir = Path("/", "tmp", "goth-tests")
    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S%z")
    log_dir = base_dir / f"goth_{date_str}"
    log_dir.mkdir(parents=True)
    return log_dir
