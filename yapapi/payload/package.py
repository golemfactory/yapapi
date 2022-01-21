import abc
import aiohttp
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


async def resolve_package_url(image_url: str, image_hash: str) -> str:
    async with aiohttp.ClientSession() as client:
        resp = await client.head(image_url, allow_redirects=True)
        if resp.status != 200:
            resp.raise_for_status()

        return f"hash:sha3:{image_hash}:{image_url}"


async def resolve_package_repo_url(repo_url: str, image_hash: str) -> str:
    async with aiohttp.ClientSession() as client:
        resp = await client.get(f"{repo_url}/image.{image_hash}.link")
        if resp.status != 200:
            resp.raise_for_status()

        image_url = await resp.text()
        return f"hash:sha3:{image_hash}:{image_url}"
