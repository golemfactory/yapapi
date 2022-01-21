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


@pytest.mark.asyncio
async def test_async_task_generation(
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
        ) as (_cmd_task, cmd_monitor, _process_container):
            # The requestor should print "task result: 3" once ...
            await cmd_monitor.wait_for_pattern("task result: 3", timeout=60)
            # ... then "task result: 2" twice ...
            for _ in range(3):
                await cmd_monitor.wait_for_pattern("task result: 2", timeout=10)
            # ... and "task result: 1" six times.
            for _ in range(6):
                await cmd_monitor.wait_for_pattern("task result: 1", timeout=10)
            await cmd_monitor.wait_for_pattern("all done!", timeout=10)
