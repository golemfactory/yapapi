import asyncio
import toml
from pathlib import Path

import yapapi


def test_version():
    with open(Path(yapapi.__file__).parents[1] / "pyproject.toml") as f:
        pyproject = toml.loads(f.read())

    assert yapapi.__version__ == pyproject["tool"]["poetry"]["version"]


def test_asyncio_fix():
    async def _asyncio_test():
        await asyncio.create_subprocess_shell("")

    yapapi.asyncio_fix()

    l = asyncio.get_event_loop()
    t = l.create_task(_asyncio_test())
    l.run_until_complete(t)
