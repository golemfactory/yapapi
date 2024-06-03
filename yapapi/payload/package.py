import abc
import logging
from typing import Optional

import aiohttp
from dataclasses import dataclass

from yapapi.payload import Payload

logger = logging.getLogger(__name__)


class PackageException(Exception):
    """Exception raised on any problems related to the package repository."""


@dataclass
class Package(Payload):
    """Description of a task package (e.g. a VM image) deployed on the provider nodes."""

    @abc.abstractmethod
    async def resolve_url(self) -> str:
        """Return package URL."""


async def check_package_url(image_url: str, image_hash: str) -> str:
    async with aiohttp.ClientSession() as client:
        resp = await client.head(image_url, allow_redirects=True)
        if resp.status != 200:
            resp.raise_for_status()

        return f"hash:sha3:{image_hash}:{image_url}"


def _sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.2f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


async def resolve_package_url(
    repo_url: str,
    image_tag: Optional[str] = None,
    image_hash: Optional[str] = None,
    image_use_https: bool = False,
    dev_mode: bool = False,
) -> str:
    params = {}

    if image_tag:
        params["tag"] = image_tag

    if image_hash:
        params["hash"] = image_hash

    if not params:
        raise PackageException(
            "Either an image_hash or an image_tag is required "
            "to resolve an image URL from the Golem Registry."
        )

    if "tag" in params and "hash" in params:
        raise PackageException(
            "Golem Registry images can be resolved by "
            "either an image_hash or by an image_tag but not both."
        )

    if dev_mode:
        # if dev, skip usage statistics, pass dev option for statistics
        params["dev"] = "true"
    else:
        params["count"] = "true"

    async with aiohttp.ClientSession() as client:
        url = f"{repo_url}/v1/image/info"
        logger.debug(f"Querying registry portal: url={url}, params={params}")
        resp = await client.get(url, params=params)
        if resp.status != 200:
            try:
                text = await resp.text()
            except Exception as ex:
                logger.error(f"Failed to get body of response: {ex}")
                text = "N/A"

            logger.error(f"Failed to resolve image URL: {resp.status} {text}")
            raise Exception(f"Failed to resolve image URL: {resp.status} {text}")

        json_resp = await resp.json()

    if image_use_https:
        image_url = json_resp["https"]
    else:
        image_url = json_resp["http"]
    image_hash = json_resp["sha3"]
    image_size = json_resp["size"]
    logger.debug(
        f"Resolved image: "
        f"url={image_url}, "
        f"size={_sizeof_fmt(image_size)}, "
        f"hash={image_hash}"
    )

    return f"hash:sha3:{image_hash}:{image_url}"
