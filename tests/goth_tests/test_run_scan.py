"""An integration test scenario that runs the `scan` example."""
import asyncio
import logging
import os
import re
import signal
from pathlib import Path
from typing import List

import pytest

from goth.configuration import Override, load_yaml
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

from .assertions import assert_all_invoices_accepted, assert_no_errors

logger = logging.getLogger("goth.test.run_test")

SUBNET_TAG = "goth"


@pytest.mark.asyncio
async def test_run_scan(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:
    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    requestor_path = project_dir / "examples" / "scan" / "scan.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):
        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --subnet-tag {SUBNET_TAG} --scan-size 3",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor, process_monitor):
            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            # ensure this line is produced twice with a differing provider and task info:
            #    Task finished by provider 'provider-N', task data: M

            providers = set()
            tasks = set()

            for i in range(2):
                output = await cmd_monitor.wait_for_pattern(
                    ".*Task finished by provider", timeout=120
                )
                matches = re.match(r".*by provider 'provider-(\d)', task data: (\d)", output)
                providers.add(matches.group(1))
                tasks.add(matches.group(2))

            assert providers == {"1", "2"}
            assert tasks == {"0", "1"}
            logger.info("Scanner tasks completed for the two providers in the network.")

            # ensure no more tasks are executed by the two providers
            logger.info("Waiting to see if another task gets started...")
            await asyncio.sleep(30)

            tasks_finished = [
                e for e in cmd_monitor._events if re.match(".*Task finished by provider", e)
            ]

            assert len(tasks_finished) == 2
            logger.info("As expected, no more tasks started. Issuing a break...")

            proc: asyncio.subprocess.Process = await process_monitor.get_process()
            proc.send_signal(signal.SIGINT)

            logger.info("SIGINT sent...")

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
            logger.info("Requestor script finished.")
