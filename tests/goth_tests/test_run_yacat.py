import logging
import math
import os
from pathlib import Path
from typing import List

import pytest

from goth.assertions import EventStream
from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from .assertions import assert_no_errors, assert_all_invoices_accepted, assert_tasks_processed


logger = logging.getLogger("goth.test.run_yacat")

EXPECTED_KEYSPACE_SIZE = 95
PROVIDER_COUNT = 2
CHUNK_SIZE = math.ceil(EXPECTED_KEYSPACE_SIZE / PROVIDER_COUNT)
ALL_TASKS = {"compute_keyspace", "0", f"{CHUNK_SIZE}"}


# Temporal assertions expressing properties of sequences of "events". In this case, each "event"
# is just a line of output from `yacat.py`.


async def assert_all_tasks_started(output_lines: EventStream[str]):
    """Assert that for every task a line with `Task started on provider` will appear."""
    await assert_tasks_processed(ALL_TASKS, "started on provider", output_lines)


async def assert_all_tasks_computed(output_lines: EventStream[str]):
    """Assert that for every task a line with `Task computed by provider` will appear."""
    await assert_tasks_processed(ALL_TASKS, "finished by provider", output_lines)


@pytest.mark.asyncio
async def test_run_yacat(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    yacat_path = project_dir / "examples" / "yacat" / "yacat.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{yacat_path} --mask ?a?a --hash $P$5ZDzPE45CigTC6EY4cXbyJSLj/pGee0 "
            f"--subnet-tag goth --chunk-size {CHUNK_SIZE} --max-workers {PROVIDER_COUNT}",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor):

            # Add assertions to the command output monitor `cmd_monitor`:
            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)
            all_sent = cmd_monitor.add_assertion(assert_all_tasks_started)
            all_computed = cmd_monitor.add_assertion(assert_all_tasks_computed)

            await cmd_monitor.wait_for_pattern(".*Received proposals from 2", timeout=10)
            logger.info("Received proposals")

            await cmd_monitor.wait_for_pattern(
                f".*The keyspace size is {EXPECTED_KEYSPACE_SIZE}", timeout=120
            )
            logger.info("Keyspace found")

            await all_sent.wait_for_result(timeout=30)
            logger.info("All tasks sent")

            await all_computed.wait_for_result(timeout=60)
            logger.info("All tasks computed")

            await cmd_monitor.wait_for_pattern(".*Password found: yo", timeout=10)
            logger.info("Password found, waiting for Executor shutdown")

            await cmd_monitor.wait_for_pattern(".*Executor has shut down", timeout=120)
            logger.info("Requestor script finished")
