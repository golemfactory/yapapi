#!/usr/bin/env python3
"""Test if a Cluster respawns an instance if an existing instance fails in the `starting` state."""
import logging
import os
from pathlib import Path

import pytest

import goth.configuration
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

logger = logging.getLogger("goth.test.async_task_generation")


@pytest.mark.asyncio
async def test_instance_restart(project_dir: Path, log_dir: Path, config_overrides) -> None:
    """Run the `requestor.py` and make sure that it's standard output is as expected."""

    configure_logging(log_dir)

    # Override the default test configuration to create only one provider node
    nodes = [
        {"name": "requestor", "type": "Requestor"},
        {"name": "provider-1", "type": "VM-Wasm-Provider", "use-proxy": True},
    ]
    config_overrides.append(("nodes", nodes))
    goth_config = goth.configuration.load_yaml(
        project_dir / "tests" / "goth" / "assets" / "goth-config.yml",
        config_overrides,
    )

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            str(Path(__file__).parent / "requestor.py"), env=os.environ
        ) as (_cmd_task, cmd_monitor):

            # The first attempt to create an instance should fail
            await cmd_monitor.wait_for_pattern(
                r".*INFO yapapi\.services\] .* commissioned$", timeout=60
            )
            await cmd_monitor.wait_for_pattern(".*CommandExecutionError", timeout=20)
            await cmd_monitor.wait_for_pattern(
                r".*INFO yapapi\.services\] .* decommissioned$", timeout=20
            )
            # The second one should succeed
            await cmd_monitor.wait_for_pattern(
                r".*INFO yapapi\.services\] .* commissioned$", timeout=30
            )
            await cmd_monitor.wait_for_pattern("STARTING$", timeout=20)
            await cmd_monitor.wait_for_pattern("RUNNING$", timeout=20)
            await cmd_monitor.wait_for_pattern(
                r".*INFO yapapi\.services\] .* decommissioned$", timeout=20
            )
