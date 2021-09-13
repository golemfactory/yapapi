"""An integration test scenario that runs custom usage counter example requestor app."""
import logging
import os
from pathlib import Path
import re
import time
from typing import List

import pytest

from goth.assertions import EventStream
from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe

from .assertions import assert_no_errors, assert_all_invoices_accepted


logger = logging.getLogger("goth.test.run_custom_usage_counter")

RUNNING_TIME = 200  # in seconds
SUBNET_TAG = "goth"


async def assert_correct_startup_and_shutdown(output_lines: EventStream[str]):
    """Assert that all providers and services that started stopped successfully."""
    providers_names = set()
    service_ids = set()

    async for line in output_lines:
        m = re.search("service ([a-zA-Z0-9]+) running on '([^']*)'", line)
        if m:
            service_id = m.group(1)
            prov_name = m.group(2)
            logger.debug("service %s running on provider '%s'", service_id, prov_name)
            service_ids.add(service_id)
            providers_names.add(prov_name)
        m = re.search("service ([a-zA-Z0-9]+) stopped on '([^']*)'", line)
        if m:
            service_id = m.group(1)
            prov_name = m.group(2)
            logger.debug("service %s stopped on provider '%s'", service_id, prov_name)
            service_ids.remove(service_id)
            providers_names.remove(prov_name)

    if providers_names:
        raise AssertionError(f"Providers not stopped: {','.join(providers_names)}")

    if service_ids:
        raise AssertionError(f"Services not stopped: {','.join(service_ids)}")


@pytest.mark.asyncio
async def test_run_custom_usage_counter(
    log_dir: Path,
    project_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    configure_logging(log_dir)

    # This is the default configuration with 2 wasm/VM providers
    goth_config = load_yaml(goth_config_path, config_overrides)
    requestor_path = project_dir / "examples" / "custom-usage-counter" / "custom_usage_counter.py"

    runner = Runner(
        base_log_dir=log_dir,
        compose_config=goth_config.compose_config,
    )

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            f"{requestor_path} --running-time {RUNNING_TIME} --subnet-tag {SUBNET_TAG}",
            env=os.environ,
        ) as (_cmd_task, cmd_monitor):

            cmd_monitor.add_assertion(assert_no_errors)
            cmd_monitor.add_assertion(assert_all_invoices_accepted)

            cmd_monitor.add_assertion(assert_correct_startup_and_shutdown)

            await cmd_monitor.wait_for_pattern(".*All jobs have finished", timeout=300)
            logger.info(f"Requestor script finished")
