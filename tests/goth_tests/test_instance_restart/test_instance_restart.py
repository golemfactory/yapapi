#!/usr/bin/env python3
"""Test if a Cluster respawns an instance if an existing instance fails in the `starting` state."""
import logging
import os
from pathlib import Path
from typing import List

import pytest

import goth.configuration
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

logger = logging.getLogger("goth.test.async_task_generation")


instances_started = set()
instances_running = set()


async def count_instances(events):
    global instances_started, instances_running

    async for line in events:
        line = line.strip()
        try:
            word, num = line.split()
            if word == "STARTING":
                instances_started.add(int(num))
            elif word == "RUNNING":
                instances_running.add(int(num))
        except ValueError:
            pass


@pytest.mark.asyncio
async def test_instance_restart(
    log_dir: Path,
    goth_config_path: Path,
    config_overrides: List[goth.configuration.Override],
) -> None:
    """Run the `requestor.py` and make sure that it's standard output is as expected."""

    configure_logging(log_dir)

    # Override the default test configuration to create only one provider node
    nodes = [
        {"name": "requestor", "type": "Requestor"},
        {"name": "provider-1", "type": "VM-Wasm-Provider", "use-proxy": True},
    ]
    config_overrides.append(("nodes", nodes))
    goth_config = goth.configuration.load_yaml(goth_config_path, config_overrides)

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            str(Path(__file__).parent / "requestor.py"), env=os.environ
        ) as (_cmd_task, cmd_monitor):

            cmd_monitor.add_assertion(count_instances)

            # The first attempt to create an instance should fail
            await cmd_monitor.wait_for_pattern("STARTING 1$", timeout=60)
            await cmd_monitor.wait_for_pattern(".*CommandExecutionError", timeout=20)

            # The second one should successfully start and fail in `running` state
            await cmd_monitor.wait_for_pattern("STARTING 2$", timeout=20)
            await cmd_monitor.wait_for_pattern("RUNNING 2$", timeout=20)
            await cmd_monitor.wait_for_pattern(".*CommandExecutionError", timeout=20)
            await cmd_monitor.wait_for_pattern("STOPPING 2$", timeout=20)

            # The third instance should be started, but not running
            await cmd_monitor.wait_for_pattern("STARTING 3$", timeout=20)
            await cmd_monitor.wait_for_pattern("Cluster stopped$", timeout=60)

            assert instances_started == {1, 2, 3}, (
                "Expected to see instances 1, 2, 3 starting, saw instances "
                f"{', '.join(str(n) for n in instances_started)} instead"
            )
            assert instances_running == {2}, (
                "Expected to see only instance 2 running, saw instances "
                f"{', '.join(str(n) for n in instances_running)} instead"
            )
