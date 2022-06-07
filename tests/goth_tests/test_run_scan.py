"""An integration test scenario that runs the `scan` example."""
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

from .assertions import assert_no_errors, assert_all_invoices_accepted


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
            f"{requestor_path} --subnet-tag {SUBNET_TAG} --scan-size 2",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor, process_monitor):
            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            # ensure this line is produced twice with a differing provider and task info:
            #    Task finished by provider 'provider-N', task data: M

            providers = set()
            tasks = set()

            for i in range(2):
                output = await cmd_monitor.wait_for_pattern(".*Task finished by provider")
                matches = re.match(".*by provider 'provider-(\d)', task data: (\d)", output)
                providers.add(matches.group(1))
                tasks.add(matches.group(2))

            assert providers == {"1", "2"}
            assert tasks == {"0", "1"}

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=60)
            logger.info(f"Requestor script finished.")
