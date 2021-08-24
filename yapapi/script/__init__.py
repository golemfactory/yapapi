"""Stuff."""

from datetime import timedelta
from typing import Optional, List

from yapapi.ctx import WorkContext
from yapapi.script.command import Command, Deploy, Start
from yapapi.script.work import BatchCommand


class Script:
    """Stuff."""

    timeout: Optional[timedelta] = None
    """Time after which this script's execution should be forcefully interrupted."""

    wait_for_results: bool = True
    """Stuff."""

    def __init__(self, context: WorkContext):
        self._ctx: WorkContext = context
        self._commands: List[Command] = []

    async def _evaluate(self) -> List[BatchCommand]:
        """Evaluate and serialize this script to a list of batch commands."""
        batch: List[BatchCommand] = []
        for step in self._commands:
            batch_cmd = await step.evaluate(self._ctx)
            batch.append(batch_cmd)
        return batch

    async def _after(self):
        """Hook which is executed after the script has been run on the provider."""
        for step in self._commands:
            await step.after(self._ctx)

    async def _before(self):
        """Hook which is executed before the script is evaluated and sent to the provider."""
        if not self._ctx._started and self._ctx._implicit_init:
            # TODO: maybe check if first two steps already cover this?
            self._commands.insert(0, Deploy())
            self._commands.insert(1, Start())

        for step in self._commands:
            await step.before(self._ctx)

    def add(self, cmd: Command) -> None:
        self._commands.append(cmd)
