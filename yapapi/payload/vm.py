import aiohttp
from dns.exception import DNSException
from dataclasses import dataclass, field
from enum import Enum
import logging
from typing import Optional
from typing_extensions import Final
from srvresolver.srv_resolver import SRVResolver, SRVRecord  # type: ignore

from yapapi.payload.package import Package, PackageException
from yapapi.props import base as prop_base
from yapapi.props.builder import DemandBuilder
from yapapi.props import inf
from yapapi.props.inf import InfBase, INF_CORES, RUNTIME_VM, ExeUnitRequest

_DEFAULT_REPO_SRV: Final = "_girepo._tcp.dev.golem.network"
_FALLBACK_REPO_URL: Final = "http://girepo.dev.golem.network:8000"

logger = logging.getLogger(__name__)


@dataclass
class InfVm(InfBase):
    runtime = RUNTIME_VM
    cores: int = prop_base.prop(INF_CORES, default=1)


InfVmKeys = InfVm.property_keys()


class VmPackageFormat(Enum):
    UNKNOWN = None
    GVMKIT_SQUASH = "gvmkit-squash"


@dataclass
class VmRequest(ExeUnitRequest):
    package_format: VmPackageFormat = prop_base.prop("golem.srv.comp.vm.package_format")


@dataclass(frozen=True)
class _VmConstraints:
    min_mem_gib: float = prop_base.constraint(inf.INF_MEM, operator=">=")
    min_storage_gib: float = prop_base.constraint(inf.INF_STORAGE, operator=">=")
    min_cpu_threads: int = prop_base.constraint(inf.INF_THREADS, operator=">=")
    # cores: int = prop_base.constraint(inf.INF_CORES, operator=">=")

    runtime: str = prop_base.constraint(inf.INF_RUNTIME_NAME, operator="=", default=RUNTIME_VM)

    def __str__(self):
        return prop_base.join_str_constraints(prop_base.constraint_model_serialize(self))


@dataclass
class _VmPackage(Package):
    repo_url: str
    image_hash: str
    constraints: _VmConstraints

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
    *,
    image_hash: str,
    min_mem_gib: float = 0.5,
    min_storage_gib: float = 2.0,
    min_cpu_threads: int = 1,
) -> Package:
    """
    Build a reference to application package.

    :param image_hash: hash of the package's image
    :param min_mem_gib: minimal memory required to execute application code
    :param min_storage_gib: minimal disk storage to execute tasks
    :param min_cpu_threads: minimal available logical CPU cores
    :return: the payload definition for the given VM image
    """
    return _VmPackage(
        repo_url=resolve_repo_srv(_DEFAULT_REPO_SRV),
        image_hash=image_hash,
        constraints=_VmConstraints(min_mem_gib, min_storage_gib, min_cpu_threads),
    )


def resolve_repo_srv(repo_srv, fallback_url=_FALLBACK_REPO_URL) -> str:
    """
    Get the url of the package repository based on its SRV record address.

    :param repo_srv: the SRV domain name
    :param fallback_url: temporary hardcoded fallback url in case there's a problem resolving SRV
    :return: the url of the package repository containing the port
    :raises: PackageException if no valid service could be reached
    """
    try:
        try:
            srv: Optional[SRVRecord] = SRVResolver.resolve_random(repo_srv)
        except DNSException as e:
            raise PackageException(f"Could not resolve Golem package repository address [{e}].")

        if not srv:
            raise PackageException("Golem package repository is currently unavailable.")
    except Exception as e:
        # this is a temporary fallback for a problem resolving the SRV record
        logger.warning(
            "Problem resolving %s, falling back to %s, exception: %s", repo_srv, fallback_url, e
        )
        return fallback_url

    return f"http://{srv.host}:{srv.port}"
