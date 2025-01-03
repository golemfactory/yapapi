"""Infrastructural properties."""

from enum import Enum
from typing import Any, Dict, List, Optional

from dataclasses import dataclass
from deprecated import deprecated

from .base import Model, prop

INF_MEM: str = "golem.inf.mem.gib"
INF_STORAGE: str = "golem.inf.storage.gib"
INF_CORES: str = "golem.inf.cpu.cores"
INF_THREADS: str = "golem.inf.cpu.threads"
TRANSFER_CAPS: str = "golem.activity.caps.transfer.protocol"
INF_RUNTIME_NAME = "golem.runtime.name"

RUNTIME_VM = "vm"


@dataclass
class WasmInterface(Enum):
    WASI_0 = "0"
    WASI_0preview1 = "0preview1"


@dataclass
class InfBase(Model):
    runtime: str = prop(INF_RUNTIME_NAME)

    mem: float = prop(INF_MEM)
    storage: Optional[float] = prop(INF_STORAGE, default=None)
    transfers: Optional[List[str]] = prop(TRANSFER_CAPS, default=None)


@dataclass
class ExeUnitRequest(Model):
    package_url: str = prop("golem.srv.comp.task_package")


@deprecated(version="0.6.0", reason="this is part of yapapi.payload.vm now")
class VmPackageFormat(Enum):
    UNKNOWN = None
    GVMKIT_SQUASH = "gvmkit-squash"


@dataclass
class ExeUnitManifestRequest(Model):
    manifest: str = prop("golem.srv.comp.payload")
    manifest_sig: Optional[str] = prop("golem.srv.comp.payload.sig", default=None)
    manifest_sig_algorithm: Optional[str] = prop(
        "golem.srv.comp.payload.sig.algorithm", default=None
    )
    manifest_cert: Optional[str] = prop("golem.srv.comp.payload.cert", default=None)
    package_format: VmPackageFormat = prop(
        "golem.srv.comp.vm.package_format", default=VmPackageFormat.GVMKIT_SQUASH
    )
    node_descriptor: Optional[Dict[str, Any]] = prop(
        "golem.!exp.gap-31.v0.node.descriptor", default=None
    )


@deprecated(version="0.6.0", reason="this is part of yapapi.payload.vm now")
@dataclass
class VmRequest(ExeUnitRequest):
    package_format: VmPackageFormat = prop("golem.srv.comp.vm.package_format")
