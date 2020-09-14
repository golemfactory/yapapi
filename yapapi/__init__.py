"""Golem Python API."""
import logging

from pathlib import Path
from single_version import get_version  # type: ignore

__version__: str = get_version("yapapi", Path(__file__).parent.parent)


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
