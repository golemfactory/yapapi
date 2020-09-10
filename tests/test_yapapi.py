import toml
from pathlib import Path

import yapapi


def test_version():
    with open(Path(yapapi.__file__).parents[1] / "pyproject.toml") as f:
        pyproject = toml.loads(f.read())

    assert yapapi.__version__ == pyproject["tool"]["poetry"]["version"]
