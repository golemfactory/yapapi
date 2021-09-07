"""An integration test scenario that runs custom usage counter example requestor app."""
import logging
import os
from pathlib import Path
import time
from typing import List

import pytest

from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from .assertions import assert_no_errors, assert_all_invoices_accepted


logger = logging.getLogger("goth.test.run_custom_usage_counter")

RUNNING_TIME = 40  # in seconds
SUBNET_TAG = "goth"


@pytest.mark.asyncio
async def test_run_custom_usage_counter(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)
    requestor_path = project_dir / "examples" / "custom-usage-counter" / "custom_usage_counter.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --running-time {RUNNING_TIME} --subnet-tag {SUBNET_TAG}",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor):

            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=200)
            logger.info(f"Requestor script finished")
