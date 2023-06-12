import asyncio
from pathlib import Path

import toml

import yapapi


def test_version():
    with open(Path(yapapi.__file__).parents[1] / "pyproject.toml") as f:
        pyproject = toml.loads(f.read())

    assert yapapi.__version__ == pyproject["tool"]["poetry"]["version"]


def test_windows_event_loop_fix():
    async def _asyncio_test():
        await asyncio.create_subprocess_shell("")

    yapapi.windows_event_loop_fix()

    loop = asyncio.get_event_loop()
    task = loop.create_task(_asyncio_test())
    loop.run_until_complete(task)
