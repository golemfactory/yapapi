"""Golem Python API."""
import asyncio
import sys
import toml

from pathlib import Path
from pkg_resources import get_distribution

from yapapi.ctx import ExecOptions, Work, WorkContext
from yapapi.engine import NoPaymentAccountError
from yapapi.executor import Executor, Task
from yapapi.golem import Golem


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


def windows_event_loop_fix():
    """
    Set up asyncio to use ProactorEventLoop implementation for new event loops on Windows.

    This work-around is only needed for Python 3.6 and 3.7.
    With Python 3.8, `ProactorEventLoop` is already the default on Windows.
    """

    if sys.platform == "win32" and sys.version_info < (3, 8):

        class _WindowsEventPolicy(asyncio.events.BaseDefaultEventLoopPolicy):
            _loop_factory = asyncio.windows_events.ProactorEventLoop

        asyncio.set_event_loop_policy(_WindowsEventPolicy())


__version__: str = get_version()
__all__ = [
    "Executor",
    "props",
    "rest",
    "executor",
    "storage",
    "Task",
    "WorkContext",
    "ExecOptions",
    "Golem",
    "NoPaymentAccountError",
    "Work",
]
