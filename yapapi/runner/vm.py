from . import Package
from dataclasses import dataclass
from typing_extensions import Final
from ..props import inf
import aiohttp

from ..props.builder import DemandBuilder
from ..props.inf import VmRequest, VmPackageFormat

_DEFAULT_REPO_URL: Final = "http://3.249.139.167:8000"


@dataclass(frozen=True)
class _VmConstrains:
    min_mem_gib: float
    min_storage_gib: float
    cores: int = 1

    def __str__(self):
        rules = "\n\t".join(
            [
                f"({inf.InfVmKeys.mem}>={self.min_mem_gib})",
                f"({inf.InfVmKeys.storage}>={self.min_storage_gib})",
                # TODO: provider should report cores.
                #
                #  f"({inf.InfVmKeys.cores}>={self.cores})",
                f"({inf.InfVmKeys.runtime}={inf.RuntimeType.VM.value})",
            ]
        )
        return f"(&{rules})"


@dataclass
class _VmPackage(Package):
    repo_url: str
    image_hash: str
    constraints: _VmConstrains

    async def resolve_url(self) -> str:
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{self.repo_url}/image.{self.image_hash}.link")
            if resp.status != 200:
                resp.raise_for_status()

            image_url = await resp.text()
            image_hash = self.image_hash
            return f"hash:sha3:{image_hash}:{image_url}"

    async def decorate_demand(self, demand: DemandBuilder):
        image_url = await self.resolve_url()
        demand.ensure(str(self.constraints))
        demand.add(VmRequest(package_url=image_url, package_format=VmPackageFormat.GVMKIT_SQUASH))


async def repo(
    *, image_hash: str, min_mem_gib: float = 0.5, min_storage_gib: float = 2.0
) -> Package:
    """
    Builds reference to application package.

    - *image_hash*: finds package by its contents hash.
    - *min_mem_gib*: minimal memory required to execute application code.
    - *min_storage_gib* minimal disk storage to execute tasks.

    """
    return _VmPackage(
        repo_url=_DEFAULT_REPO_URL,
        image_hash=image_hash,
        constraints=_VmConstrains(min_mem_gib, min_storage_gib),
    )
