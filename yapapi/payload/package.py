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


async def resolve_package_repo_url(repo_url: str, image: str) -> str:
    if len(image) == 56:
        # SHA3_224 hash
        image_url = f"{repo_url}/v1/image/download?hash={image}"
        return f"hash:sha3:{image}:{image_url}"
    else:
        async with aiohttp.ClientSession() as session:
            # Send request to Golem Registry to get image SHA3_224 hash
            async with session.get(f"{repo_url}/v1/image/info?tag={image}", allow_redirects=True) as response:
                data = await response.json()
                image_hash = data["sha3"]
                image_url = f"{repo_url}/v1/image/download?tag={image}"
                return f"hash:sha3:{image_hash}:{image_url}"
