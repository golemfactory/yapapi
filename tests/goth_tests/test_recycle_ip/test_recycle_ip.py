"""An integration test scenario that runs the SSH example requestor app."""
import asyncio
import logging
import os
from pathlib import Path
import pexpect
import pytest
import re
import signal
import sys
import time
from typing import List

from goth.configuration import Override, load_yaml
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

from goth_tests.assertions import assert_all_invoices_accepted, assert_no_errors  # isort:skip

logger = logging.getLogger("goth.test.recycle_ips")

SUBNET_TAG = "goth"


@pytest.mark.asyncio
async def test_recycle_ip(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
    ssh_verify_connection: bool,
) -> None:

    if ssh_verify_connection:
        ssh_check = pexpect.spawn("/usr/bin/which ssh")
        exit_code = ssh_check.wait()
        if exit_code != 0:
            raise ProcessLookupError(
                "ssh binary not found, please install it or check your PATH. "
                "You may also skip the connection check by omitting `--ssh-verify-connection`."
            )

    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)

    # disable the mitm proxy used to capture the requestor agent -> daemon API calls
    # because it doesn't support websockets which are neeeded to enable VPN connections
    requestor = [c for c in goth_config.containers if c.name == "requestor"][0]
    requestor.use_proxy = False

    requestor_path = str(Path(__file__).parent / "ssh_recycle_ip.py")

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
            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            ssh_connections = []

            # A longer timeout to account for downloading a VM image
            ssh_string = await cmd_monitor.wait_for_pattern("ssh -o", timeout=120)
            matches = re.match("ssh -o .* -p (\\d+) (root@.*)", ssh_string)
            port = matches.group(1)
            address = matches.group(2)
            password = re.sub("password: ", "", await cmd_monitor.wait_for_pattern("password:"))

            ssh_connections.append((port, address, password))

            await cmd_monitor.wait_for_pattern(".*SshService running on provider", timeout=10)
            logger.info("SSH service instances started")

            if not ssh_verify_connection:
                logger.warning(
                    "Skipping SSH connection check. Use `--ssh-verify-connection` to perform it."
                )
            else:
                for port, address, password in ssh_connections:
                    args = [
                        "ssh",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                        "-o",
                        "StrictHostKeyChecking=no",
                        "-p",
                        port,
                        address,
                        "uname -v",
                    ]

                    logger.debug("running ssh with: %s", args)

                    ssh = pexpect.spawn(" ".join(args))
                    ssh.expect("[pP]assword:", timeout=5)
                    ssh.sendline(password)
                    ssh.expect("#1-Alpine SMP", timeout=5)
                    ssh.expect(pexpect.EOF, timeout=5)
                    logger.info("Connection to port %s confirmed.", port)

                logger.info("SSH connections confirmed.")

            proc: asyncio.subprocess.Process = await process_monitor.get_process()
            proc.send_signal(signal.SIGINT)
            logger.info("Sent SIGINT...")
