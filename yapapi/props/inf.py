"""Infrastructural properties."""

from typing import Optional, List
from dataclasses import dataclass
from deprecated import deprecated  # type: ignore
from enum import Enum
from .base import Model, prop

INF_MEM: str = "golem.inf.mem.gib"
INF_STORAGE: str = "golem.inf.storage.gib"
INF_CORES: str = "golem.inf.cpu.cores"
INF_THREADS: str = "golem.inf.cpu.threads"
TRANSFER_CAPS: str = "golem.activity.caps.transfer.protocol"
INF_RUNTIME_NAME = "golem.runtime.name"

RUNTIME_WASMTIME = "wasmtime"
RUNTIME_EMSCRIPTEN = "emscripten"
RUNTIME_VM = "vm"


@dataclass
@deprecated(
    version="0.6.0",
    reason="please use yapapi.props.inf.RUNTIME_* constants directly",
    action="default",
)
class RuntimeType(Enum):
    UNKNOWN = ""
    WASMTIME = RUNTIME_WASMTIME
    EMSCRIPTEN = RUNTIME_EMSCRIPTEN
    VM = RUNTIME_VM


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
@deprecated(version="0.6.0", reason="this is part of yapapi.payload.vm now")
class InfVm(InfBase):
    runtime = RUNTIME_VM
    cores: int = prop(INF_CORES, default=1)
    threads: int = prop(INF_THREADS, default=1)


InfVmKeys = InfVm.property_keys()


@dataclass
class ExeUnitRequest(Model):
    package_url: str = prop("golem.srv.comp.task_package")


@deprecated(version="0.6.0", reason="this is part of yapapi.payload.vm now")
class VmPackageFormat(Enum):
    UNKNOWN = None
    GVMKIT_SQUASH = "gvmkit-squash"


@deprecated(version="0.6.0", reason="this is part of yapapi.payload.vm now")
@dataclass
class VmRequest(ExeUnitRequest):
    package_format: VmPackageFormat = prop("golem.srv.comp.vm.package_format")
