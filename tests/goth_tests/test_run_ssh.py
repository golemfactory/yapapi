"""An integration test scenario that runs the Simple Service example requestor app."""
import asyncio
import logging
import os
from pathlib import Path
import signal
import time
from typing import List

import pytest

from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from .assertions import assert_no_errors, assert_all_invoices_accepted


logger = logging.getLogger("goth.test.run_ssh")

SUBNET_TAG = "goth"


@pytest.mark.asyncio
async def test_run_ssh(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    requestor_path = project_dir / "examples" / "ssh" / "ssh.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --subnet-tag {SUBNET_TAG}",
            env=os.environ,
            get_process_monitor=True,
        ) as (_cmd_task, cmd_monitor, process_container):
            start_time = time.time()

            def elapsed_time():
                return f"time: {(time.time() - start_time):.1f}"

            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            # # A longer timeout to account for downloading a VM image
            for i in range(2):
                ssh_string = await cmd_monitor.wait_for_pattern("ssh -o ProxyCommand", timeout=120)
                password = await cmd_monitor.wait_for_pattern("password:")
                print("---------------------- ", ssh_string, password)

            await cmd_monitor.wait_for_pattern(
                ".*SshService running on provider.*SshService running on provider", timeout=10)

            proc: asyncio.subprocess.Process = await process_container.get_process()
            proc.send_signal(signal.SIGINT)
            logger.info("Sent SIGINT...")

            for _ in range(2):
                await cmd_monitor.wait_for_pattern(
                    ".*SshService terminated on provider", timeout=120)

            logger.info(f"The instances have been terminated ({elapsed_time()})")

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
            logger.info(f"Requestor script finished ({elapsed_time()})")
