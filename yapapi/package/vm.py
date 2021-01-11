import aiohttp
from dataclasses import dataclass
from typing_extensions import Final

from srvresolver.srv_resolver import SRVResolver, SRVRecord  # type: ignore

from yapapi.package import Package, PackageException
from yapapi.props.builder import DemandBuilder
from yapapi.props.inf import InfVmKeys, RuntimeType, VmRequest, VmPackageFormat

_DEFAULT_REPO_SRV: Final = "_girepo._tcp.dev.golem.network"


@dataclass(frozen=True)
class _VmConstrains:
    min_mem_gib: float
    min_storage_gib: float
    cores: int = 1

    def __str__(self):
        rules = "\n\t".join(
            [
                f"({InfVmKeys.mem}>={self.min_mem_gib})",
                f"({InfVmKeys.storage}>={self.min_storage_gib})",
                # TODO: provider should report cores.
                #
                #  f"({inf.InfVmKeys.cores}>={self.cores})",
                f"({InfVmKeys.runtime}={RuntimeType.VM.value})",
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
        repo_url=resolve_repo_srv(_DEFAULT_REPO_SRV),
        image_hash=image_hash,
        constraints=_VmConstrains(min_mem_gib, min_storage_gib),
    )


def resolve_repo_srv(repo_srv) -> str:
    srv: SRVRecord = SRVResolver.resolve_random(repo_srv)
    if not srv:
        raise PackageException("Golem package repository is currently unavailable.")
    return f"http://{srv.host}:{srv.port}"
