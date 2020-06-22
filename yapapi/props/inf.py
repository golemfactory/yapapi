"""
Infrastructural Properties

"""

from typing import Optional, List
from dataclasses import dataclass, field
from .base import Model

INF_MEM: str = "golem.inf.mem.gib"
INF_STORAGE: str = "golem.inf.storage.gib"
INF_CORES: str = "golem.inf.cpu.cores"
TRANSFER_CAPS: str = "golem.activity.caps.transfer.protocol"


@dataclass
class InfBase(Model):
    mem: float = field(metadata={"key": INF_MEM})
    runtime: str = field(metadata={"key": "golem.runtime.name"})

    storage: Optional[float] = field(default=None, metadata={"key": INF_STORAGE})
    transfers: Optional[List[str]] = field(
        default=None, metadata={"key": TRANSFER_CAPS}
    )


@dataclass
class InfVm(Model):
    cores: int = field(metadata={"key": INF_CORES})
