"""A goth test scenario for multi-activity agreements."""

import logging
import os
import re
from functools import partial
from pathlib import Path
from typing import List

import pytest

import goth.configuration
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

logger = logging.getLogger("goth.test.multiactivity_agreement")


async def assert_agreement_created(events):
    """Assert that `AgreementCreated` event occurs."""

    async for line in events:
        m = re.match(r"AgreementCreated\(.*Agreement\(id=([0-9a-f]+)", line)
        if m:
            return m.group(1)
    raise AssertionError("Expected AgreementCreated event")


async def assert_multiple_workers_run(agr_id, events):
    """Assert that more than one worker is run with given `agr_id`.

    Fails if a worker failure is detected or if a worker has run for another agreement.
    """
    workers_finished = 0

    async for line in events:
        m = re.match(r"WorkerFinished\(.*Agreement\(id=([0-9a-f]+)", line)
        if m:
            worker_agr_id = m.group(1)
            assert worker_agr_id == agr_id, "Worker run for another agreement"
            assert "exception" not in line, "Worker finished with error"
            workers_finished += 1
        elif re.match("JobFinished", line):
            break

    assert workers_finished > 1, (
        f"Only {workers_finished} worker(s) run for agreement {agr_id}, " "expected more than one"
    )


@pytest.mark.asyncio
async def test_multiactivity_agreement(
    log_dir: Path,
    goth_config_path: Path,
    config_overrides: List[goth.configuration.Override],
    single_node_override: goth.configuration.Override,
) -> None:
    configure_logging(log_dir)

    goth_config = goth.configuration.load_yaml(
        goth_config_path, config_overrides + [single_node_override]
    )

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    async with runner(goth_config.containers):
        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(
            str(Path(__file__).parent / "requestor.py"), env=os.environ
        ) as (_cmd_task, cmd_monitor, _process_monitor):
            # Wait for agreement
            assertion = cmd_monitor.add_assertion(assert_agreement_created)
            agr_id = await assertion.wait_for_result(timeout=30)

            # Wait for multiple workers run for the agreement
            assertion = cmd_monitor.add_assertion(
                partial(assert_multiple_workers_run, agr_id),
                name=f"assert_multiple_workers_run({agr_id})",
            )
            await assertion.wait_for_result(timeout=60)
