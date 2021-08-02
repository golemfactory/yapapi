import logging
import os
from pathlib import Path
from typing import List

import pytest

from goth.assertions import EventStream
from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from yapapi.log import SummaryLogger

from .assertions import assert_no_errors, assert_all_invoices_accepted, assert_tasks_processed


logger = logging.getLogger("goth.test.run_blender")

ALL_TASKS = {"0", "10", "20", "30", "40", "50"}


# Temporal assertions expressing properties of sequences of "events". In this case, each "event"
# is just a line of output from `blender.py`.


async def assert_all_tasks_started(output_lines: EventStream[str]):
    """Assert that for every task a line with `Task started on provider` will appear."""
    await assert_tasks_processed(ALL_TASKS, "started on provider", output_lines)


async def assert_all_tasks_computed(output_lines: EventStream[str]):
    """Assert that for every task a line with `Task computed by provider` will appear."""
    await assert_tasks_processed(ALL_TASKS, "finished by provider", output_lines)


@pytest.mark.asyncio
async def test_run_blender(
    project_dir: Path, log_dir: Path, goth_config_path: Path, config_overrides: List[Override]
) -> None:

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    blender_path = project_dir / "examples" / "blender" / "blender.py"

    configure_logging(log_dir)

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{blender_path} --subnet-tag goth",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor):

            # Add assertions to the command output monitor `cmd_monitor`:
            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)
            all_sent = cmd_monitor.add_assertion(assert_all_tasks_started)
            all_computed = cmd_monitor.add_assertion(assert_all_tasks_computed)

            await cmd_monitor.wait_for_pattern(".*Received proposals from 2 ", timeout=20)
            logger.info("Received proposals")

            await cmd_monitor.wait_for_pattern(".*Agreement proposed ", timeout=10)
            logger.info("Agreement proposed")

            await cmd_monitor.wait_for_pattern(".*Agreement confirmed ", timeout=10)
            logger.info("Agreement confirmed")

            await all_sent.wait_for_result(timeout=120)
            logger.info("All tasks sent")

            await all_computed.wait_for_result(timeout=120)
            logger.info("All tasks computed, waiting for Golem shutdown")

            await cmd_monitor.wait_for_pattern(
                f".*{SummaryLogger.GOLEM_SHUTDOWN_SUCCESSFUL_MESSAGE}", timeout=120
            )

            logger.info("Requestor script finished")
