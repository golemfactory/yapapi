from deprecated import deprecated  # type: ignore
import logging

from yapapi.payload.package import Package
from yapapi.payload.vm import (
    repo as _repo,
    resolve_repo_srv as _resolve_repo_srv,
    _FALLBACK_REPO_URL,
)

logger = logging.getLogger(__name__)


@deprecated(version="0.6.0", reason="moved to yapapi.payload.vm.repo")
async def repo(
    *, image_hash: str, min_mem_gib: float = 0.5, min_storage_gib: float = 2.0
) -> Package:
    """
    Build reference to application package.

    - *image_hash*: finds package by its contents hash.
    - *min_mem_gib*: minimal memory required to execute application code.
    - *min_storage_gib* minimal disk storage to execute tasks.

    """
    return await _repo(
        image_hash=image_hash, min_mem_gib=min_mem_gib, min_storage_gib=min_storage_gib
    )


@deprecated(version="0.6.0", reason="moved to yapapi.payload.vm.resolve_repo_srv")
def resolve_repo_srv(repo_srv, fallback_url=_FALLBACK_REPO_URL) -> str:
    """
    Get the url of the package repository based on its SRV record address.

    :param repo_srv: the SRV domain name
    :param fallback_url: temporary hardcoded fallback url in case there's a problem resolving SRV
    :return: the url of the package repository containing the port
    :raises: PackageException if no valid service could be reached
    """
    return _resolve_repo_srv(repo_srv=repo_srv, fallback_url=fallback_url)
