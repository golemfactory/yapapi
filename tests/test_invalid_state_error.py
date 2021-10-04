import pytest

from yapapi.rest.activity import CommandExecutionError
from yapapi.events import CommandExecuted


@pytest.mark.parametrize(
    "src_command, src_msg, additional_msg",
    (
        (
            {"deploy": {}},
            "whatever",
            "",
        ),
        (
            {"deploy": {}},
            "Local service error: State error: Invalid state: StatePair(Ready, None)",
            "\nThere was already a succesful start() - sending deploy() is not allowed",
        ),
        (
            {"start": {}},
            "Local service error: State error: Invalid state: StatePair(Ready, None)",
            "\nThere was already a succesful start() - sending start() is not allowed",
        ),
        (
            {"run": {}},
            "Local service error: State error: Invalid state: StatePair(Initialized, None)",
            "\nActivity is initialized - deploy() command is expected now",
        ),
        (
            {"run": {}},
            "Local service error: State error: Invalid state: StatePair(Deployed, None)",
            "\nActivity is deployed - start() command is expected now",
        ),
        (
            {"run": {}},
            "Local service error: State error: Invalid state: StatePair(Terminating, None)",
            "\nThis command is not allowed when activity is in state Terminating",
        ),
    ),
)
def test_invalid_state_error(src_command, src_msg, additional_msg):
    evt = CommandExecuted(
        job_id=0,
        agr_id=0,
        script_id=0,
        cmd_idx=0,
        command=src_command,
        success=False,
        message=src_msg,
    )

    #   Compare: yapapi.engine._Engine.process_batches.get_batch_results
    e = CommandExecutionError(evt.command, evt.message, evt.stderr)

    expected_message = (
        f"Command '{src_command}' failed on provider; message: '{src_msg}'{additional_msg}"
    )
    assert str(e) == expected_message
