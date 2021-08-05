"""A goth test scenario for agreement termination."""
from functools import partial
import logging
import os
from pathlib import Path
import re
from typing import List

import pytest

from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe


logger = logging.getLogger("goth.test.agreement_termination")


async def assert_command_error(stream):
    """Assert that a worker failure due to `CommandExecutionError` is reported."""

    async for line in stream:
        m = re.match(r"WorkerFinished\(.*agr_id='([^']+)'.*CommandExecutionError", line)
        if m:
            return m.group(1)
    raise AssertionError("Expected CommandExecutionError failure")


async def assert_agreement_cancelled(agr_id, stream):
    """Assert that the agreement with the given id is eventually terminated.

    Fails if a new task is started for the agreement.
    """

    async for line in stream:
        if re.match(rf"TaskStarted\(.*agr_id='{agr_id}'", line):
            raise AssertionError(f"Task started for agreement {agr_id}")
        if re.match(
            rf"AgreementTerminated\(.*agr_id='{agr_id}'.*'golem.requestor.code': 'Cancelled'", line
        ):
            return


async def assert_all_tasks_computed(stream):
    """Assert that for every task id, `TaskAccepted` with that id occurs."""
    remaining_ids = {1, 2, 3, 4, 5, 6}

    async for line in stream:
        m = re.search(r"TaskAccepted\(.*task_id='([0-9]+)'", line)
        if m:
            task_id = int(m.group(1))
            logger.debug("assert_all_tasks_computed: Task %d computed", task_id)
            remaining_ids.discard(task_id)
        if not remaining_ids:
            return

    raise AssertionError(f"Tasks not computed: {remaining_ids}")


@pytest.mark.asyncio
async def test_agreement_termination(
    log_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)
    test_script_path = str(Path(__file__).parent / "requestor.py")

    configure_logging(log_dir)

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(test_script_path, env=os.environ) as (
            _cmd_task,
            cmd_monitor,
        ):

            cmd_monitor.add_assertion(assert_all_tasks_computed)

            # Wait for worker failure due to command error
            assertion = cmd_monitor.add_assertion(assert_command_error)
            agr_id = await assertion.wait_for_result(timeout=60)
            logger.info("Detected command error in activity for agreement %s", agr_id)

            # Make sure no new tasks are sent and the agreement is terminated
            assertion = cmd_monitor.add_assertion(
                partial(assert_agreement_cancelled, agr_id),
                name=f"assert_agreement_cancelled({agr_id})",
            )
            await assertion.wait_for_result(timeout=10)

            # Wait for executor shutdown
            await cmd_monitor.wait_for_pattern("ShutdownFinished", timeout=60)
            logger.info("Requestor script finished")
