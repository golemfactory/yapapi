#!/usr/bin/env python3
"""Test concurrent job execution and that SummaryLogger handles it correctly."""
import logging
import os
from pathlib import Path
from typing import List

import pytest

import goth.configuration
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

logger = logging.getLogger("goth.test.async_concurrent_executors")


@pytest.mark.asyncio
async def test_concurrent_executors(
    log_dir: Path,
    goth_config_path: Path,
    config_overrides: List[goth.configuration.Override],
) -> None:
    """Run the `requestor.py` and make sure that it's standard output is as expected."""

    configure_logging(log_dir)

    goth_config = goth.configuration.load_yaml(goth_config_path, config_overrides)

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            str(Path(__file__).parent / "requestor.py"), env=os.environ
        ) as (_cmd_task, cmd_monitor, _process_monitor):

            # Wait for job ALEF summary
            await cmd_monitor.wait_for_pattern(".*ALEF.* Computation finished", timeout=60)
            await cmd_monitor.wait_for_pattern(".*ALEF.* Negotiated 2 agreements", timeout=5)
            await cmd_monitor.wait_for_pattern(".*ALEF.* Provider .* computed 8 tasks", timeout=5)
            await cmd_monitor.wait_for_pattern(".*ALEF.* Activity failed 1 time", timeout=5)

            # Wait for job BET summary
            await cmd_monitor.wait_for_pattern(".*BET.* Computation finished", timeout=60)
            await cmd_monitor.wait_for_pattern(".*BET.* Negotiated 1 agreement", timeout=5)
            await cmd_monitor.wait_for_pattern(".*BET.* Provider .* computed 8 tasks", timeout=5)

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
