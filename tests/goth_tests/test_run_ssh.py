"""An integration test scenario that runs the SSH example requestor app."""
import asyncio
import logging
import os
from pathlib import Path
import pexpect
import re
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
    ssh_verify_connection: bool,
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
        ) as (_cmd_task, cmd_monitor, process_monitor):
            start_time = time.time()

            def elapsed_time():
                return f"time: {(time.time() - start_time):.1f}"

            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            ssh_connections = []

            # # A longer timeout to account for downloading a VM image
            for i in range(2):
                ssh_string = await cmd_monitor.wait_for_pattern("ssh -o ProxyCommand", timeout=120)
                matches = re.match("ssh -o ProxyCommand=('.*') (root@.*)", ssh_string)

                # the default API port goes through a proxy that logs REST requests
                # but does not support websocket connections
                # hence, we're replacing it with a port that connects directly
                # to the daemon's port in the requestor's Docker container
                proxy_cmd = re.sub(":16(\\d\\d\\d)", ":6\\1", matches.group(1))

                auth_str = matches.group(2)
                password = re.sub("password: ", "", await cmd_monitor.wait_for_pattern("password:"))

                ssh_connections.append((proxy_cmd, auth_str, password))

            await cmd_monitor.wait_for_pattern(
                ".*SshService running on provider.*SshService running on provider", timeout=10
            )

            if not ssh_verify_connection:
                logger.warning(
                    "Skipping SSH connection check. Use `--ssh-verify-connection` to perform it."
                )
            else:
                for proxy_cmd, auth_str, password in ssh_connections:
                    args = [
                        "ssh",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                        "-o",
                        "StrictHostKeyChecking=no",
                        "-o",
                        "ProxyCommand=" + proxy_cmd,
                        auth_str,
                        "uname -v",
                    ]

                    logger.debug("running ssh with: %s", args)

                    ssh = pexpect.spawn(" ".join(args))
                    ssh.expect("[pP]assword:", timeout=5)
                    ssh.sendline(password)
                    ssh.expect("#1-Alpine SMP", timeout=5)
                    ssh.expect(pexpect.EOF, timeout=5)
                    logger.info("Connection to %s confirmed.", auth_str)

                logger.info("SSH connections confirmed.")

            proc: asyncio.subprocess.Process = await process_monitor.get_process()
            proc.send_signal(signal.SIGINT)
            logger.info("Sent SIGINT...")

            for _ in range(2):
                await cmd_monitor.wait_for_pattern(
                    ".*SshService terminated on provider", timeout=120
                )

            logger.info(f"The instances have been terminated ({elapsed_time()})")

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=20)
            logger.info(f"Requestor script finished ({elapsed_time()})")
