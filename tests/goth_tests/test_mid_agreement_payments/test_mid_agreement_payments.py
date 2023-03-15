"""A goth test scenario for mid-agreement payments."""
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pytest

from goth.assertions import EventStream
from goth.configuration import Override, load_yaml
from goth.runner import Runner
from goth.runner.log import configure_logging
from goth.runner.probe import RequestorProbe

DEBIT_NOTE_INTERVAL_GRACE_PERIOD = 30

logger = logging.getLogger("goth.test.mid_agreement_payments")


async def assert_debit_note_freq(events: EventStream) -> str:
    expected_note_num: int = 1
    received_freq: Optional[int] = None
    note_num: int = 0
    async for log_line in events:
        if received_freq is None:
            m = re.search(r"Debit notes interval: ([0-9]+)", log_line)
            if m:
                received_freq = int(m.group(1))

        m = re.search(
            r"Debit notes for activity.* ([0-9]+) notes/([0-9]+)",
            log_line,
        )
        if m and "Payable" not in log_line:
            note_num = int(m.group(1))
            total_time = int(m.group(2))

            assert note_num == expected_note_num, "Unexpected debit note number"
            expected_note_num += 1
            assert (
                received_freq is not None
            ), "Expected debit note frequency message before a debit note"
            assert (
                total_time + DEBIT_NOTE_INTERVAL_GRACE_PERIOD > note_num * received_freq
            ), "Too many notes"
    assert note_num >= 2, "Expected at least two debit notes"
    return f"{note_num} debit notes processed"


@pytest.mark.asyncio
async def test_mid_agreement_payments(
    project_dir: Path,
    log_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
    single_node_override: Override,
) -> None:
    # goth setup
    config = load_yaml(goth_config_path, config_overrides + [single_node_override])
    configure_logging(log_dir)

    logfile = f"mid-agreement-payments-{datetime.now().strftime('%Y-%m-%d_%H.%M.%S')}.log"

    requestor_path = (
        project_dir / "tests" / "goth_tests" / "test_mid_agreement_payments" / "requestor_agent.py"
    )

    runner = Runner(base_log_dir=log_dir, compose_config=config.compose_config)
    async with runner(config.containers):
        # given
        requestor = runner.get_probes(probe_type=RequestorProbe)[0]
        # when
        async with requestor.run_command_on_host(
            f"{requestor_path} --log-file {(log_dir / logfile).resolve()}", env=os.environ
        ) as (
            _,
            cmd_monitor,
            _,
        ):
            # then
            cmd_monitor.add_assertion(assert_debit_note_freq)
            # assert mid-agreement payments were enabled
            await cmd_monitor.wait_for_pattern(".*Enabling mid-agreement payments.*", timeout=60)
            # Wait for executor shutdown
            await cmd_monitor.wait_for_pattern(".*ShutdownFinished.*", timeout=200)
            logger.info("Requestor script finished")
