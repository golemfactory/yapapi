"""Unit tests for `yapapi.compose.parser`.

Values used in `assert` calls are taken from test YAML files located in `./yaml` directory.
"""

import pytest
from pathlib import Path
from typing import List

from yapapi.compose import parser


@pytest.fixture
def compose_yamls(request: pytest.FixtureRequest) -> List[Path]:
    test_module_path = Path(request.fspath)
    yaml_dir_path = test_module_path.parent / "yaml"
    return [yaml_dir_path / "base.yml", yaml_dir_path / "override.yml"]


def test_override_payment(compose_yamls: List[Path]):
    result = parser.load_yamls(compose_yamls)
    payment = result["payment"]
    # For keys existing in both files, the last value should be the final one
    assert payment["budget"] == 1
    # Keys existing only in the base file should be carried over to the result dict
    assert payment["driver"] == "polygon"


def test_override_payloads(compose_yamls: List[Path]):
    result = parser.load_yamls(compose_yamls)
    payload = result["payloads"]["nginx"]

    # Lists should be concatenated with respect to the order of the config files
    assert payload["capabilities"] == ["vpn", "gpu"]
    # New values should be added to existing dicts
    assert payload["params"]["repo"] == "repo-url"
    # While existing dict values should still be there
    assert payload["params"]["image"] == "image-hash"
