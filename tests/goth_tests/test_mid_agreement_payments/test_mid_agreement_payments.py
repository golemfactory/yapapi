"""A goth test scenario for mid-agreement payments."""
from functools import partial
import logging
import os
from pathlib import Path
import re
from typing import List

import pytest

from goth.configuration import load_yaml, Override
from goth.runner.log import configure_logging
from goth.runner import Runner
from goth.runner.probe import RequestorProbe
from yapapi.strategy import DEBIT_NOTE_INTERVAL_GRACE_PERIOD


logger = logging.getLogger("goth.test.mid_agreement_payments")


async def check_debit_note_freq(events):
    freq = None
    expected_note_num = 1
    total_time = 0
    note_num = None
    async for line in events:
        if freq is None:
            m = re.search(r"Debit notes interval: ([0-9]+)", line)
            if m:
                freq = int(m.group(1))
        m = re.search(r"Debit notes for activity.* ([0-9]+) notes/([0-9]+)", line)
        if m and "Payable" not in line:
            note_num = int(m.group(1))
            total_time = int(m.group(2))
            assert note_num == expected_note_num, "Unexpected debit note number"
            expected_note_num += 1
            assert freq is not None, "Expected debit note frequency message before a debit note"
            assert total_time + DEBIT_NOTE_INTERVAL_GRACE_PERIOD > note_num * freq, "Too many notes"
    assert note_num is not None and note_num >= 2, "Expected at least two debit notes"
    return f"{note_num} debit notes processed"


@pytest.mark.asyncio
async def test_mid_agreement_payments(
    log_dir: Path,
    goth_config_path: Path,
    config_overrides: List[Override],
) -> None:

    # Override the default test configuration to create only one provider node
    nodes = [
        {"name": "requestor", "type": "Requestor"},
        {"name": "provider-1", "type": "VM-Wasm-Provider", "use-proxy": True},
    ]
    config_overrides.append(("nodes", nodes))
    goth_config = load_yaml(goth_config_path, config_overrides)
    test_script_path = str(Path(__file__).parent / "requestor.py")

    configure_logging(log_dir)

    runner = Runner(base_log_dir=log_dir, compose_config=goth_config.compose_config)

    async with runner(goth_config.containers):

        requestor = runner.get_probes(probe_type=RequestorProbe)[0]

        async with requestor.run_command_on_host(test_script_path, env=os.environ) as (
            _cmd_task,
            cmd_monitor,
            _process_monitor,
        ):
            cmd_monitor.add_assertion(check_debit_note_freq)
            await cmd_monitor.wait_for_pattern(".*Enabling mid-agreement payments.*", timeout=60)
            # Wait for executor shutdown
            await cmd_monitor.wait_for_pattern(".*ShutdownFinished.*", timeout=200)
            logger.info("Requestor script finished")
