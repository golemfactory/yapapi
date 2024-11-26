import json
import logging
from enum import Enum
from typing import Final, List, Literal, Optional

from dataclasses import dataclass

from yapapi.payload.package import Package, PackageException, check_package_url, resolve_package_url
from yapapi.props import base as prop_base
from yapapi.props import inf
from yapapi.props.builder import DemandBuilder, Model
from yapapi.props.inf import INF_CORES, RUNTIME_VM, ExeUnitManifestRequest, ExeUnitRequest, InfBase

logger = logging.getLogger(__name__)

DEFAULT_REPOSITORY_URL: Final[str] = "https://registry.golem.network"

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
    """
    Remove package_format from here since it's already defined in ExeUnitManifestRequest
    """

    node_descriptor: Optional[str] = prop_base.prop(
        "golem.!exp.gap-31.v0.node.descriptor", default=None
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


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
    node_descriptor: Optional[dict]
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
                node_descriptor=self.node_descriptor,
                package_format=VmPackageFormat.GVMKIT_SQUASH,
            )
        )


import base64
from typing import Union


def is_base64(s: Union[str, bytes]) -> bool:
    """Check if input is base64 encoded."""
    if isinstance(s, bytes):
        s = s.decode("utf-8", errors="ignore")
    try:
        # Add padding if necessary
        padding = 4 - (len(s) % 4)
        if padding != 4:
            s = s + "=" * padding
        # Try to decode the string - if it succeeds and produces valid bytes, it's base64
        decoded = base64.b64decode(s)
        return True
    except Exception:
        return False


async def manifest(
    *,
    manifest: Union[str, bytes],
    manifest_sig: Optional[Union[str, bytes]] = None,
    manifest_sig_algorithm: Optional[str] = None,
    manifest_cert: Optional[Union[str, bytes]] = None,
    node_descriptor: Optional[dict] = None,
    min_mem_gib: float = 0.5,
    min_storage_gib: float = 2.0,
    min_cpu_threads: int = 1,
    capabilities: Optional[List[VmCaps]] = None,
) -> Package:
    """
    Build a reference to application payload.

    There are two approaches to handle outbound network access in Golem:

    1. Recommended: Partner Scheme (using node_descriptor)
        Uses a signed node descriptor along with a manifest to grant access to either whitelisted
        domains or unrestricted access. Providers only need to trust the certificate once.

        example usage::

            package = await vm.manifest(
                manifest = open("manifest_partner_unrestricted.json", "rb").read(),
                node_descriptor = json.loads(open("node-descriptor.signed.json", "r").read()),
                capabilities = ["inet"],
            )

    2. Alternative: Pure Manifest Scheme
        Requires providers to manually trust each domain listed in the manifest. More complex to set up
        and maintain.

        example usage::

            package = await vm.manifest(
                manifest = open("manifest_whitelist.json", "rb").read(),
                manifest_sig = open("manifest.json.sha256.sig", "rb").read(),
                manifest_sig_algorithm = "sha256",
                manifest_cert = open("cert.der", "rb").read(),
                capabilities = ["inet", "manifest-support"],
            )

    For more information about outbound access schemes, see:
    https://handbook.golem.network/requestor-tutorials/vm-runtime/accessing-internet

    Parameters:
    :param manifest: Computation Payload Manifest as raw data or base64 encoded string
    :param manifest_sig: Optional signature of manifest (required for pure manifest scheme)
    :param manifest_sig_algorithm: Optional signature algorithm, e.g. "sha256" (required for pure manifest scheme)
    :param manifest_cert: Optional public certificate for manifest verification (required for pure manifest scheme)
    :param node_descriptor: Optional signed node descriptor (recommended for partner scheme)
    :param min_mem_gib: Minimal memory required to execute application code
    :param min_storage_gib: Minimal disk storage to execute tasks
    :param min_cpu_threads: Minimal available logical CPU cores
    :param capabilities: Optional list of required VM capabilities. Use ["inet"] for partner scheme
        or ["inet", "manifest-support"] for pure manifest scheme
    :return: The payload definition for the given VM image

    The manifest, manifest_sig, and manifest_cert parameters can be provided either as raw data
    or already base64 encoded. The function will automatically handle the encoding if needed.
    """

    # Helper function to handle encoding
    def ensure_base64(data: Optional[Union[str, bytes]]) -> Optional[str]:
        if data is None:
            return None

        # If it's already base64 encoded and in string form, return as is
        if isinstance(data, str) and is_base64(data):
            return data

        # Convert to bytes if needed
        if isinstance(data, str):
            data = data.encode("utf-8")

        # Encode and convert to string
        return base64.b64encode(data).decode("utf-8")

    # Encode all inputs
    manifest_encoded = ensure_base64(manifest)
    manifest_sig_encoded = ensure_base64(manifest_sig)
    manifest_cert_encoded = ensure_base64(manifest_cert)

    capabilities = capabilities or list()
    constraints = _VmConstraints(min_mem_gib, min_storage_gib, min_cpu_threads, capabilities)

    return _VmManifestPackage(
        manifest=manifest_encoded,
        manifest_sig=manifest_sig_encoded,
        manifest_sig_algorithm=manifest_sig_algorithm,
        manifest_cert=manifest_cert_encoded,
        node_descriptor=node_descriptor,
        constraints=constraints,
    )


@dataclass
class _VmPackage(Package):
    image_url: str
    constraints: _VmConstraints

    async def resolve_url(self) -> str:
        return self.image_url

    async def decorate_demand(self, demand: DemandBuilder):
        demand.ensure(str(self.constraints))
        demand.add(
            VmRequest(package_url=self.image_url, package_format=VmPackageFormat.GVMKIT_SQUASH)
        )


async def repo(
    *,
    image_hash: Optional[str] = None,
    image_tag: Optional[str] = None,
    image_url: Optional[str] = None,
    image_use_https: bool = False,
    repository_url: str = DEFAULT_REPOSITORY_URL,
    dev_mode: bool = False,  # noqa
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
    :param repository_url: override the package repository location
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
            # only run on provider nodes with a certain number of CPU threads available
            min_cpu_threads=min_cpu_threads,
        )

    """

    if image_url:
        if image_tag:
            raise PackageException(
                "An image_tag can only be used when resolving from Golem Registry, "
                "not with a direct image_url."
            )
        if not image_hash:
            raise PackageException("An image_hash is required when using a direct image_url.")
        logger.debug(f"Verifying if {image_url} exists.")
        resolved_image_url = await check_package_url(image_url, image_hash)
    else:
        logger.debug(
            f"Resolving image on {repository_url}: "
            f"image_hash={image_hash}, image_tag={image_tag}, image_use_https={image_use_https}."
        )
        resolved_image_url = await resolve_package_url(
            repository_url,
            image_hash=image_hash,
            image_tag=image_tag,
            image_use_https=image_use_https,
            dev_mode=dev_mode,
        )

    logger.debug(f"Resolved image: {resolved_image_url}")

    capabilities = capabilities or list()
    return _VmPackage(
        image_url=resolved_image_url,
        constraints=_VmConstraints(min_mem_gib, min_storage_gib, min_cpu_threads, capabilities),
    )
