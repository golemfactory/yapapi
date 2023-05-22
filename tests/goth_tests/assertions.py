"""Temporal assertions used in `goth` integration tests.

The assertions express properties of lines of output printed by requestor
scripts (e.g. `blender.py`) to their stdout or stderr. For example, one such
property would be that if the requestor script prints a line containing
the string "Agreement confirmed by provider P", for any name P, then
eventually it will also print a line containing "Accepted invoice from P".
"""
import logging
import re
from typing import Set

from goth.assertions import EventStream

logger = logging.getLogger("goth.test.assertions")


async def assert_no_errors(output_lines: EventStream[str]):
    """Assert that no output line contains the substring `ERROR`."""
    async for line in output_lines:
        if "ERROR" in line:
            raise AssertionError("Command reported ERROR")


async def assert_all_invoices_accepted(output_lines: EventStream[str]):
    """Assert that an invoice is accepted for every provider that confirmed an agreement."""
    unpaid_agreement_providers = list()

    async for line in output_lines:
        m = re.search("Agreement confirmed by provider '([^']*)'", line)
        if m:
            prov_name = m.group(1)
            logger.debug("assert_all_invoices_accepted: adding provider '%s'", prov_name)
            unpaid_agreement_providers.append(prov_name)
        m = re.search("Accepted invoice from '([^']*)'", line)
        if m:
            prov_name = m.group(1)
            logger.debug("assert_all_invoices_accepted: adding invoice for '%s'", prov_name)
            unpaid_agreement_providers.remove(prov_name)

    if unpaid_agreement_providers:
        raise AssertionError(f"Unpaid agreements for: {','.join(unpaid_agreement_providers)}")


async def assert_tasks_processed(tasks: Set[str], status: str, output_lines: EventStream[str]):
    """Assert that for every task in `tasks` a line with `Task {status}` will appear."""
    remaining_tasks = tasks.copy()

    async for line in output_lines:
        m = re.search(rf".*Task {status} .* task data: (.+)$", line)
        if m:
            task_data = m.group(1)
            logger.debug("assert_tasks_processed: Task %s: %s", status, task_data)
            remaining_tasks.discard(task_data)
        if not remaining_tasks:
            return

    raise AssertionError(f"Tasks not {status}: {remaining_tasks}")
