"""
Golem File Transfer Storage Provider
"""

from enum import Enum


class _Command(Enum):
    EXPORT = "export"
    IMPORT = "import"
    LIST = "list"
    DONE = "done"
