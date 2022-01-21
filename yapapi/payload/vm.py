from dns.exception import DNSException
from dataclasses import dataclass, field
from enum import Enum
import logging
import sys
from typing import Optional, List
from typing_extensions import Final

if sys.version_info > (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from srvresolver.srv_resolver import SRVResolver, SRVRecord  # type: ignore

from yapapi.payload.package import (
    Package,
    PackageException,
    resolve_package_url,
    resolve_package_repo_url,
)
from yapapi.props import base as prop_base
from yapapi.props.builder import DemandBuilder
from yapapi.props import inf
from yapapi.props.inf import InfBase, INF_CORES, RUNTIME_VM, ExeUnitRequest

_DEFAULT_REPO_SRV: Final = "_girepo._tcp.dev.golem.network"
_FALLBACK_REPO_URL: Final = "http://girepo.dev.golem.network:8000"

logger = logging.getLogger(__name__)

VM_CAPS_VPN: str = "vpn"

VmCaps = Literal["vpn"]


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

    capabilities: List[VmCaps] = prop_base.constraint(
        "golem.runtime.capabilities", operator="=", default_factory=list
    )

    runtime: str = prop_base.constraint(inf.INF_RUNTIME_NAME, operator="=", default=RUNTIME_VM)

    def __str__(self):
        return prop_base.join_str_constraints(prop_base.constraint_model_serialize(self))


@dataclass
class _VmPackage(Package):
    repo_url: str
    image_hash: str
    image_url: Optional[str]
    constraints: _VmConstraints

    async def resolve_url(self) -> str:
        if not self.image_url:
            return await resolve_package_repo_url(self.repo_url, self.image_hash)
        return await resolve_package_url(self.image_url, self.image_hash)

    async def decorate_demand(self, demand: DemandBuilder):
        image_url = await self.resolve_url()
        demand.ensure(str(self.constraints))
        demand.add(VmRequest(package_url=image_url, package_format=VmPackageFormat.GVMKIT_SQUASH))


async def repo(
    *,
    image_hash: str,
    image_url: Optional[str] = None,
    min_mem_gib: float = 0.5,
    min_storage_gib: float = 2.0,
    min_cpu_threads: int = 1,
    capabilities: Optional[List[VmCaps]] = None,
) -> Package:
    """
    Build a reference to application package.

    :param image_hash: hash of the package's image
    :param image_url: URL of the package's image
    :param min_mem_gib: minimal memory required to execute application code
    :param min_storage_gib: minimal disk storage to execute tasks
    :param min_cpu_threads: minimal available logical CPU cores
    :param capabilities: an optional list of required vm capabilities
    :return: the payload definition for the given VM image

    example usage::

        package = await vm.repo(
            # if we provide only the image hash, the image will be
            # automatically pulled from Golem's image repository
            image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
        )

    example usage with an explicit GVMI image URL (useful to host images outside the Golem repository)::

        package = await vm.repo(
            # we still need to provide the image's hash because
            # the image's integrity is validated by the runtime on the provider node
            #
            # the hash can be calculated by running `sha3sum -a 224 <image_filename.gvmi>`
            #
            image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",

            # the URL can point to any publicly-available location on the web
            image_url="http://girepo.dev.golem.network:8000/docker-golem-hello-world-latest-779758b432.gvmi",
        )

    example usage with additional constraints::

        package = await vm.repo(
            image_hash="9a3b5d67b0b27746283cb5f287c13eab1beaa12d92a9f536b747c7ae",
            # only run on provider nodes that have more than 0.5gb of RAM available
            min_mem_gib=0.5,
            # only run on provider nodes that have more than 2gb of storage space available
            min_storage_gib=2.0,
            # only run on provider nodes which a certain number of CPU threads available
            min_cpu_threads=min_cpu_threads,
        )

    """
    capabilities = capabilities or list()
    return _VmPackage(
        repo_url=resolve_repo_srv(_DEFAULT_REPO_SRV),
        image_hash=image_hash,
        image_url=image_url,
        constraints=_VmConstraints(min_mem_gib, min_storage_gib, min_cpu_threads, capabilities),
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
