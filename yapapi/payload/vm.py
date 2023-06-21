import logging
import sys

from enum import Enum

from typing import List, Optional

from dataclasses import dataclass

if sys.version_info > (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from yapapi.config import ApiConfig
from yapapi.payload.package import Package, check_package_url, resolve_package_url
from yapapi.props import base as prop_base
from yapapi.props import inf
from yapapi.props.builder import DemandBuilder, Model
from yapapi.props.inf import INF_CORES, RUNTIME_VM, ExeUnitManifestRequest, ExeUnitRequest, InfBase

logger = logging.getLogger(__name__)

VM_CAPS_VPN: str = "vpn"
VM_CAPS_MANIFEST_SUPPORT: str = "manifest-support"

VmCaps = Literal["vpn", "inet", "manifest-support"]


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


@dataclass
class VmManifestRequest(ExeUnitManifestRequest):
    package_format: VmPackageFormat = prop_base.prop(
        "golem.srv.comp.vm.package_format", default=VmPackageFormat.GVMKIT_SQUASH
    )


@dataclass(frozen=True)
class _VmConstraints(Model):
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
class _VmManifestPackage(Package):
    manifest: str
    manifest_sig: Optional[str]
    manifest_sig_algorithm: Optional[str]
    manifest_cert: Optional[str]
    constraints: _VmConstraints

    async def resolve_url(self) -> str:
        return ""

    async def decorate_demand(self, demand: DemandBuilder):
        demand.ensure(str(self.constraints))
        demand.add(
            VmManifestRequest(
                manifest=self.manifest,
                manifest_sig=self.manifest_sig,
                manifest_sig_algorithm=self.manifest_sig_algorithm,
                manifest_cert=self.manifest_cert,
                package_format=VmPackageFormat.GVMKIT_SQUASH,
            )
        )


async def manifest(
    manifest: str,
    manifest_sig: Optional[str] = None,
    manifest_sig_algorithm: Optional[str] = None,
    manifest_cert: Optional[str] = None,
    min_mem_gib: float = 0.5,
    min_storage_gib: float = 2.0,
    min_cpu_threads: int = 1,
    capabilities: Optional[List[VmCaps]] = None,
) -> Package:
    """
    Build a reference to application payload.

    :param manifest: base64 encoded Computation Payload Manifest
        https://handbook.golem.network/requestor-tutorials/vm-runtime/computation-payload-manifest
    :param manifest_sig: an optional signature of base64 encoded Computation Payload Manifest
    :param manifest_sig_algorithm: an optional signature algorithm, e.g. "sha256"
    :param manifest_cert: an optional base64 encoded public certificate (DER or PEM) matching key
        used to generate signature
    :param min_mem_gib: minimal memory required to execute application code
    :param min_storage_gib: minimal disk storage to execute tasks
    :param min_cpu_threads: minimal available logical CPU cores
    :param capabilities: an optional list of required VM capabilities
    :return: the payload definition for the given VM image

    example usage::

        package = await vm.manifest(
            manifest = open("manifest.json.base64", "r").read(),
        )

    example usage with a signed Computation Pyload Manifest and additional "inet" capability::

        package = await vm.manifest(
            manifest = open("manifest.json.base64", "r").read(),
            manifest_sig = open("manifest.json.sig.base64", "r").read(),
            manifest_sig_algorithm = "sha256",
            manifest_cert = open("cert.der.base64", "r").read(),
            capabilities = ["manifest-support", "inet"],
        )
    """
    capabilities = capabilities or list()
    constraints = _VmConstraints(min_mem_gib, min_storage_gib, min_cpu_threads, capabilities)

    return _VmManifestPackage(
        manifest=manifest,
        manifest_sig=manifest_sig,
        manifest_sig_algorithm=manifest_sig_algorithm,
        manifest_cert=manifest_cert,
        constraints=constraints,
    )


@dataclass
class _VmPackage(Package):
    image_url: str
    constraints: _VmConstraints

    async def resolve_url(self) -> str:
        # trivial implementation - already resolved before creation of _VmPackage
        return self.image_url

    async def decorate_demand(self, demand: DemandBuilder):
        demand.ensure(str(self.constraints))
        demand.add(VmRequest(package_url=self.image_url,
                             package_format=VmPackageFormat.GVMKIT_SQUASH))


async def repo(
    *,
    image_hash: Optional[str] = None,
    image_tag: Optional[str] = None,
    image_url: Optional[str] = None,
    image_use_https: bool = False,
    min_mem_gib: float = 0.5,
    min_storage_gib: float = 2.0,
    min_cpu_threads: int = 1,
    capabilities: Optional[List[VmCaps]] = None,
) -> Package:
    """
    Build a reference to application package.

    :param image_hash: hash of the package's image
    :param image_tag: Tag of the package to resolve from Golem Registry
    :param image_url: URL of the package's image
    :param image_use_https: whether to resolve to HTTPS or HTTP when using Golem Registry
    :param min_mem_gib: minimal memory required to execute application code
    :param min_storage_gib: minimal disk storage to execute tasks
    :param min_cpu_threads: minimal available logical CPU cores
    :param capabilities: an optional list of required VM capabilities
    :return: the payload definition for the given VM image

    example usage::

        package = await vm.repo(
            # if we provide only the image hash, the image will be
            # automatically pulled from Golem's image repository
            image_hash="d646d7b93083d817846c2ae5c62c72ca0507782385a2e29291a3d376",
        )

    example usage with an explicit GVMI image URL (useful to host images outside the Golem
    repository)::

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

    repo_url = ApiConfig().repository_url
    if not image_tag and not image_hash:
        raise ValueError("Either image_tag or image_hash must be provided")
    elif image_tag and image_url:
        raise ValueError(
            "You cannot override image_url when using image_tag, use image_hash instead")
    elif not image_url and image_hash:
        logger.info(f"Resolving using {repo_url} by image hash {image_hash}")
        resolved_image_url = await resolve_package_url(repo_url,
                                                       image_hash=image_hash,
                                                       image_use_https=image_use_https)
    elif not image_url and image_tag:
        logger.info(f"Resolving using {repo_url} by image tag {image_tag}")
        resolved_image_url = await resolve_package_url(repo_url,
                                                       image_tag=image_tag,
                                                       image_use_https=image_use_https)
    elif image_hash and image_url:
        logger.info(f"Checking if image url is correct for {image_url} and {image_hash}")
        resolved_image_url = await check_package_url(image_url, image_hash)
    else:
        raise ValueError("Invalid combination of arguments")

    logger.info(f"Resolved image full link: {resolved_image_url}")

    capabilities = capabilities or list()
    return _VmPackage(
        image_url=resolved_image_url,
        constraints=_VmConstraints(min_mem_gib, min_storage_gib, min_cpu_threads, capabilities),
    )
