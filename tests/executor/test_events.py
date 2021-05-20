"""Unit tests for the `yapapi.executor.events` module."""
from typing import Any

import pytest

from yapapi.executor.events import CommandEventContext, CommandExecuted


@pytest.mark.parametrize(
    "message, expected_stdout, expected_stderr",
    [
        ('{"stdout": null, "stderr": null}', None, None),
        ('{"stderr":null, "stdout": "some\\noutput"}', "some\noutput", None),
        ('{"stdout": null, "stderr": "stderr", "extra": "extra"}', None, "stderr"),
        # Error values
        (None, None, None),
        ("[not a valid JSON...", None, None),
        ('{"stdout": "some output", "stderr": error}', None, None),
        ('{"std_out": "some output", "stderr": "error"}', None, None),
        ('{"stdout": "just some output"}', "just some output", None),
    ],
)
def test_command_executed_stdout_stderr(
    message: Any,
    expected_stdout: str,
    expected_stderr: str,
) -> None:
    """Check if command's stdout and stderr are correctly extracted from `message` argument."""

    kwargs = {"cmd_idx": 1, "command": "mock-command", "message": message}
    ctx = CommandEventContext(CommandExecuted, kwargs)
    e = ctx.event(agr_id="2c3b6f473b86fd923591ec568df4797", script_id="2", cmds=[1, 2, 3, 4, 5])

    assert isinstance(e, CommandExecuted)
    assert e.stdout == expected_stdout
    assert e.stderr == expected_stderr
