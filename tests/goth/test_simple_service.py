"""An integration test scenario that runs the Simple Service example requestor app."""
import logging
import os
from pathlib import Path
import time

import pytest

from goth.configuration import load_yaml
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from .assertions import assert_no_errors, assert_all_invoices_accepted


logger = logging.getLogger("goth.test.run_simple_service")


@pytest.mark.asyncio
async def test_run_simple_service(log_dir: Path, project_dir: Path, config_overrides) -> None:

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(
        Path(__file__).parent / "assets" / "goth-config.yml", overrides=config_overrides
    )

    requestor_path = project_dir / "examples" / "simple-service-poc" / "simple_service.py"

    configure_logging(log_dir)

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --running-time 40 --subnet-tag goth",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor):

            start_time = time.time()

            def elapsed_time():
                return f"time: {(time.time() - start_time):.1f}"

            # Add assertions to the command output monitor `cmd_monitor`:
            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            await cmd_monitor.wait_for_pattern("Starting 1 instance", timeout=20)
            await cmd_monitor.wait_for_pattern("instances:.*'starting'", timeout=20)
            logger.info(f"The instance is starting ({elapsed_time()})")

            # A longer timeout to account for downloading a VM image
            await cmd_monitor.wait_for_pattern("All instances started", timeout=120)
            logger.info(f"The instance was started successfully ({elapsed_time()})")

            for _ in range(3):
                await cmd_monitor.wait_for_pattern("instances:.*'running'", timeout=20)
                logger.info("The instance is running")

            await cmd_monitor.wait_for_pattern("Stopping instances", timeout=60)
            logger.info(f"The instance is stopping ({elapsed_time()})")

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
            logger.info(f"Requestor script finished ({elapsed_time()}")
