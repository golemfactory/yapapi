"""Utilities for logging computation events"""
import json
import logging
from typing import Any

from yapapi.runner.events import EventType, event_type_to_string


logger = logging.getLogger("yapapi.runner")


def log_event(event: EventType, level=logging.DEBUG) -> None:
    """Log an event in human-readable format."""

    def _format(obj: Any, max_len: int = 200) -> str:
        # This will also escape control characters, in particular,
        # newline characters in `obj` will be replaced by r"\n".
        text = repr(obj)
        if len(text) > max_len:
            text = text[: max_len - 3] + "..."
        return text

    if not logger.isEnabledFor(level):
        return

    msg = event_type_to_string[type(event)]
    info = "; ".join(f"{name} = {_format(value)}" for name, value in event._asdict().items())
    if info:
        msg += "; " + info
    logger.log(level, msg)


def log_event_json(event: EventType) -> None:
    """Log an event as a tag with attributes in JSON format."""

    info = {name: str(value) for name, value in event._asdict().items()}
    logger.debug("%s %s", type(event).__name__, json.dumps(info) if info else "")
