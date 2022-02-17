"""An integration test scenario that runs the Simple Service example requestor app."""
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


logger = logging.getLogger("goth.test.run_simple_service")

RUNNING_TIME = 40  # in seconds
SUBNET_TAG = "goth"


@pytest.mark.asyncio
@pytest.mark.skip
async def test_run_simple_service(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    requestor_path = project_dir / "examples" / "simple-service-poc" / "simple_service.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --running-time {RUNNING_TIME} --subnet-tag {SUBNET_TAG}",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor, _process_monitor):

            start_time = time.time()

            def elapsed_time():
                return f"time: {(time.time() - start_time):.1f}"

            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            await cmd_monitor.wait_for_pattern(
                "Starting Cluster 1: 1x\\[Service: SimpleService", timeout=20
            )
            # A longer timeout to account for downloading a VM image
            await cmd_monitor.wait_for_pattern("All instances started", timeout=120)
            logger.info(f"The instance was started successfully ({elapsed_time()})")

            for _ in range(3):
                await cmd_monitor.wait_for_pattern("instances:.*running", timeout=20)
                logger.info("The instance is running")

            await cmd_monitor.wait_for_pattern(
                "Stopping Cluster 1: 1x\\[Service: SimpleService", timeout=60
            )
            logger.info(f"The instance is stopping ({elapsed_time()})")

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
            logger.info(f"Requestor script finished ({elapsed_time()})")
