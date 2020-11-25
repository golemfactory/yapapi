"""Infrastructural properties."""

from typing import Optional, List
from dataclasses import dataclass, field
from enum import Enum
from .base import Model

INF_MEM: str = "golem.inf.mem.gib"
"""the key which defines the memory requirement"""

INF_STORAGE: str = "golem.inf.storage.gib"
"""the key which defines the storage requirement"""

INF_CORES: str = "golem.inf.cpu.cores"
"""the key which defines number of required CPU cores"""

TRANSFER_CAPS: str = "golem.activity.caps.transfer.protocol"
"""the key which defines the transfer protocol"""

@dataclass
class RuntimeType(Enum):
    """Enum of possible runtime environments."""
    UNKNOWN = ""
    """Unknown runtime"""
    WASMTIME = "wasmtime"
    """Wasmtime runtime"""
    EMSCRIPTEN = "emscripten"
    """Emscripten runtime"""
    VM = "vm"
    """VM container runtime"""


@dataclass
class WasmInterface(Enum):
    """Wasi version"""
    WASI_0 = "0"
    """0"""
    WASI_0preview1 = "0preview1"
    """0preview1"""


@dataclass
class InfBase(Model):
    """Base model describing the possible runtimes and the related requirements."""
    mem: float = field(metadata={"key": INF_MEM})
    """memory requirement"""
    runtime: RuntimeType = field(metadata={"key": "golem.runtime.name"})
    """runtime environment"""

    storage: Optional[float] = field(default=None, metadata={"key": INF_STORAGE})
    """storage requirement"""
    transfers: Optional[List[str]] = field(default=None, metadata={"key": TRANSFER_CAPS})
    """required transfer protocol"""


@dataclass
class InfVm(InfBase):
    """Model that describes the VM runtime requirement."""
    runtime = RuntimeType.VM
    cores: int = field(default=1, metadata={"key": INF_CORES})
    """required number of cores"""


InfVmKeys = InfVm.keys()


@dataclass
class ExeUnitRequest(Model):
    """Model describing the requirements for an execution unit."""
    package_url: str = field(metadata={"key": "golem.srv.comp.task_package"})
    """package used for computation"""


class VmPackageFormat(Enum):
    """Enum of possible VM package formats."""
    UNKNOWN = None
    """unknown"""
    GVMKIT_SQUASH = "gvmkit-squash"
    """gvmkit-build's image format"""


@dataclass
class VmRequest(ExeUnitRequest):
    """Requirements for the VM execution unit."""
    package_format: VmPackageFormat = field(metadata={"key": "golem.srv.comp.vm.package_format"})
    """package format"""
