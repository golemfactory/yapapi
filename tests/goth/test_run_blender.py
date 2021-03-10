import asyncio
import logging
import os
from pathlib import Path

import pytest

from goth.configuration import load_yaml
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe


logger = logging.getLogger("goth.test")


@pytest.mark.asyncio
async def test_run_blender(
    log_dir: Path,
    project_dir: Path,
) -> None:

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(Path(__file__).parent / "assets" / "goth-config.yml")

    blender_path = project_dir / "examples" / "blender" / "blender.py"

    configure_logging(log_dir)

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        agent_task = requestor.run_command_on_host(
            f"{blender_path} --subnet-tag goth",
            env=os.environ,
        )

        while not agent_task.done():
            logger.info("Waiting for requestor script to complete...")
            await asyncio.sleep(5)

        logger.info("Requestor script finished")
