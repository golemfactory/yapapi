import abc
import aiohttp
from dataclasses import dataclass
from typing import Optional
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


async def resolve_package_repo_url(repo_url: str, image_hash: Optional[str] = None, image_name: Optional[str] = None) -> str:
    if image_hash:
        if len(image_hash) == 56:
            # SHA3_224 hash
            image_url = f"{repo_url}/v1/image/download?hash={image_hash}"
            return f"hash:sha3:{image_hash}:{image_url}"
        else:
            raise PackageException(
                f"Invalid image hash: {image_hash}. "
                "The hash must be a SHA3_224 hash of the image file."
            )
    elif image_name:
        async with aiohttp.ClientSession() as session:
            # Send request to Golem Registry to get image SHA3_224 hash
            async with session.get(f"{repo_url}/v1/image/info?tag={image_name}", allow_redirects=True) as response:
                if response.status == 404:
                    raise PackageException(f"Image not found: {image_name}")
                elif response.status != 200:
                    raise PackageException(f"Error while getting image info: {response.status}")  
                data = await response.json()
                image_hash = data["sha3"]
                image_url = f"{repo_url}/v1/image/download?tag={image_name}"
                return f"hash:sha3:{image_hash}:{image_url}"
    else:
        raise PackageException("Either an image hash or an image name must be provided.")