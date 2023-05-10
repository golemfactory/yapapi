import logging
import os
from pathlib import Path
from typing import List

import pytest

from goth.configuration import Override, load_yaml
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

logger = logging.getLogger("goth.test.renegotiate_proposal")


@pytest.mark.asyncio
async def test_renegotiation(
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
            await cmd_monitor.wait_for_pattern(r"\[.+\] Renegotiating", timeout=50)
            await cmd_monitor.wait_for_pattern(r"agreement.terminate\(\): True", timeout=50)
            # assert not "Main timeout triggered :("
            await cmd_monitor.wait_for_pattern(r"All done", timeout=50)
