"""A goth test scenario for negotiating long expiration time."""
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


logger = logging.getLogger("goth.test.long_expiration")


@pytest.mark.asyncio
async def test_long_expiration(
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
            _process_monitor,
        ):

            # cmd_monitor.add_assertion(assert_all_tasks_computed)

            # # Wait for worker failure due to command error
            # assertion = cmd_monitor.add_assertion(assert_command_error)
            # agr_id = await assertion.wait_for_result(timeout=60)
            # logger.info("Detected command error in activity for agreement %s", agr_id)

            # # Make sure no new tasks are sent and the agreement is terminated
            # assertion = cmd_monitor.add_assertion(
            #     partial(assert_agreement_cancelled, agr_id),
            #     name=f"assert_agreement_cancelled({agr_id})",
            # )
            # await assertion.wait_for_result(timeout=10)

            # Wait for executor shutdown
            await cmd_monitor.wait_for_pattern("ShutdownFinished", timeout=900)
            logger.info("Requestor script finished")
