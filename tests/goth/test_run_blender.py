import asyncio
import logging
import os
from pathlib import Path

import pytest

from goth.configuration import Configuration, load_yaml
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe


logger = logging.getLogger("goth.test")


@pytest.fixture()
def configuration() -> Configuration:
    return load_yaml(Path(__file__).parent / "assets" / "goth-config.yml")


@pytest.fixture()
def log_dir() -> Path:
    dir = Path(__file__).parent / "goth-logs"
    dir.mkdir(exist_ok=True)
    return dir


@pytest.mark.asyncio
async def test_run_blender(
    configuration: Configuration,
    log_dir: Path,
) -> None:

    configure_logging(log_dir)

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=configuration.compose_config,
    )

    async with runner(configuration.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        agent_task = requestor.run_command_on_host(
            f"examples/blender/blender.py --subnet-tag goth",
            env=os.environ,
        )

        while not agent_task.done():
            logger.info("Waiting for requestor script to complete...")
            await asyncio.sleep(5)

        logger.info("Requestor script finished")
