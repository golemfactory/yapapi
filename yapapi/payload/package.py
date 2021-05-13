import abc
from dataclasses import dataclass

from yapapi.payload import Payload


class PackageException(Exception):
    """Exception raised on any problems related to the package repository."""

    pass


@dataclass  # type: ignore  # mypy issue #5374
class Package(Payload):
    """Description of a task package (e.g. a VM image) deployed on the provider nodes"""

    @abc.abstractmethod
    async def resolve_url(self) -> str:
        """Return package URL."""
