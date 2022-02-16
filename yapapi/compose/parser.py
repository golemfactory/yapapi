from pathlib import Path
from typing import Any, Dict, List

import dpath.util
import yaml


def load_yamls(yaml_paths: List[Path]) -> Dict[str, Any]:
    """Loads the provided YAML files, merging their contents in a deep manner.

    The order of the files is relevant, that is: the first YAML is considered the base.
    All of the remaining files are loaded one by one and deeply merged into the base.

    Returns a dict representing the result of all YAML files merged into the first one.
    """

    def _load_yaml(path: Path) -> Dict[str, Any]:
        with path.open() as f:
            return yaml.load(f, yaml.SafeLoader)

    base_dict = _load_yaml(yaml_paths[0])
    for path in yaml_paths[1:]:
        dpath.util.merge(
            base_dict, _load_yaml(path), flags=dpath.util.MERGE_ADDITIVE | dpath.util.MERGE_TYPESAFE
        )

    return base_dict
