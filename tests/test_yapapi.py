import toml
from pathlib import Path

from yapapi import __version__


def test_version():
    with open(Path(__file__).parents[1] / "pyproject.toml") as f:
        pyproject = toml.loads(f.read())

    assert __version__ == pyproject["tool"]["poetry"]["version"]
