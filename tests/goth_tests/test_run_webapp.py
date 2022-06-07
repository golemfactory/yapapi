"""An integration test scenario that runs the SSH example requestor app."""
import asyncio
import logging
import os
from pathlib import Path
import signal
import time
from typing import List

import pytest
import requests

from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from .assertions import assert_no_errors, assert_all_invoices_accepted


logger = logging.getLogger("goth.test")

SUBNET_TAG = "goth"

ONELINER_ENTRY = "hello from goth"
ONELINER_URL = "http://localhost:8080/"


@pytest.mark.asyncio
async def test_run_webapp(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:
    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    # disable the mitm proxy used to capture the requestor agent -> daemon API calls
    # because it doesn't support websockets which are needed by the VPN (and the Local HTTP Proxy)
    requestor = [c for c in goth_config.containers if c.name == "requestor"][0]
    requestor.use_proxy = False

    requestor_path = project_dir / "examples" / "webapp" / "webapp.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --subnet-tag {SUBNET_TAG}",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor, process_monitor):
            start_time = time.time()

            def elapsed_time():
                return f"time: {(time.time() - start_time):.1f}"

            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            logger.info("Waiting for the instances to start")

            # A longer timeout to account for downloading a VM image
            await cmd_monitor.wait_for_pattern("DB instance started.*", timeout=240)
            logger.info("Db instance started")

            await cmd_monitor.wait_for_pattern("Local HTTP server listening on.*", timeout=120)
            logger.info("HTTP instance started")

            requests.post(ONELINER_URL, data={"message": ONELINER_ENTRY})
            r = requests.get(ONELINER_URL)
            assert r.status_code == 200
            assert ONELINER_ENTRY in r.text
            logger.info("DB write confirmed :)")

            proc: asyncio.subprocess.Process = await process_monitor.get_process()
            proc.send_signal(signal.SIGINT)
            logger.info("Sent SIGINT...")

            for i in range(2):
                await cmd_monitor.wait_for_pattern(".*Service terminated.*", timeout=20)

            logger.info(f"The instances have been terminated ({elapsed_time()})")

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
            logger.info(f"Requestor script finished ({elapsed_time()})")
