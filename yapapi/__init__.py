"""Golem Python API."""
import logging

from pathlib import Path
from pkg_resources import get_distribution


def enable_default_logger(
    format_: str = "[%(asctime)s %(levelname)s %(name)s] %(message)s", level: int = logging.INFO
):
    """Enable the default logger that logs to stderr."""

    logger = logging.getLogger("yapapi")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(format_))
    handler.setLevel(level)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.disabled = False


def get_version() -> str:
    pyproject_path = Path(__file__).parents[1] / "pyproject.toml"
    if pyproject_path.exists():
        import toml

        with open(pyproject_path) as f:
            pyproject = toml.loads(f.read())

        return pyproject["tool"]["poetry"]["version"]

    return get_distribution("yapapi").version


__version__: str = get_version()
