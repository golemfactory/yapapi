"""Golem Python API."""
import asyncio
import sys
import toml

from pathlib import Path
from pkg_resources import get_distribution

from .executor import Executor, Task, WorkContext


def get_version() -> str:
    """
    :return: the version of the yapapi library package
    """
    pyproject_path = Path(__file__).parents[1] / "pyproject.toml"
    if pyproject_path.exists():
        with open(pyproject_path) as f:
            pyproject = toml.loads(f.read())

        return pyproject["tool"]["poetry"]["version"]

    return get_distribution("yapapi").version


def asyncio_fix():
    if sys.platform == "win32":

        class _WindowsEventPolicy(asyncio.events.BaseDefaultEventLoopPolicy):
            _loop_factory = asyncio.windows_events.ProactorEventLoop

        asyncio.set_event_loop_policy(_WindowsEventPolicy())


__version__: str = get_version()
__all__ = ["Executor", "props", "rest", "executor", "storage", "Task", "WorkContext"]
