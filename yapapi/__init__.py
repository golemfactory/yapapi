"""Golem Python API."""
import logging


def enable_default_logger(
        format_: str = "[%(asctime)s %(levelname)s %(name)s] %(message)s",
        level: int = logging.INFO
):
    """Enable the default logger that logs to stderr."""

    logger = logging.getLogger("yapapi")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(format_))
    handler.setLevel(level)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.disabled = False
