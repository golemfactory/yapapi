"""
Infrastructural Properties

"""

from typing import Optional, List
from dataclasses import dataclass, field
from enum import Enum
from .base import Model

INF_MEM: str = "golem.inf.mem.gib"
INF_STORAGE: str = "golem.inf.storage.gib"
INF_CORES: str = "golem.inf.cpu.cores"
TRANSFER_CAPS: str = "golem.activity.caps.transfer.protocol"


@dataclass
class RuntimeType(Enum):
    UNKNOWN = ""
    WASMTIME = "wasmtime"
    EMSCRIPTEN = "emscripten"
    VM = "vm"


@dataclass
class WasmInterface(Enum):
    WASI_0 = "0"
    WASI_0preview1 = "0preview1"


@dataclass
class InfBase(Model):
    mem: float = field(metadata={"key": INF_MEM})
    runtime: RuntimeType = field(metadata={"key": "golem.runtime.name"})

    storage: Optional[float] = field(default=None, metadata={"key": INF_STORAGE})
    transfers: Optional[List[str]] = field(default=None, metadata={"key": TRANSFER_CAPS})


@dataclass
class InfVm(InfBase):
    runtime = RuntimeType.VM
    cores: int = field(default=1, metadata={"key": INF_CORES})


InfVmKeys = InfVm.keys()


@dataclass
class ExeUnitRequest(Model):
    package_url: str = field(metadata={"key": "golem.srv.comp.task_package"})


class VmPackageFormat(Enum):
    UNKNOWN = None
    GVMKIT_SQUASH = "gvmkit-squash"


@dataclass
class VmRequest(ExeUnitRequest):
    package_format: VmPackageFormat = field(metadata={"key": "golem.srv.comp.vm.package_format"})
