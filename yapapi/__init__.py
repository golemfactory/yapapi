"""Golem Python API."""
import toml

from pathlib import Path
from pkg_resources import get_distribution

from .runner import Executor, Task, WorkContext


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


__version__: str = get_version()
__all__ = ["Executor", "props", "rest", "runner", "storage", "Task", "WorkContext"]
