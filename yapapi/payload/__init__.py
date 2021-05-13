import abc

from yapapi.props.builder import AutodecoratingModel


class Payload(AutodecoratingModel, abc.ABC):
    """Base class for descriptions of the payload required by the requestor.

    example:
    ```python
    >>> import asyncio
    >>>
    >>> from dataclasses import dataclass
    >>>
    >>> from yapapi.props.builder import DemandBuilder
    >>> from yapapi.props.base import prop, constraint
    >>> from yapapi.props import inf
    >>>
    >>> from yapapi.payload import Payload
    >>>
    >>>
    >>> TURBOGETH_RUNTIME_NAME = "turbogeth-managed"
    >>> PROP_TURBOGETH_CHAIN = "golem.srv.app.eth.chain"
    >>>
    >>>
    >>> @dataclass
    ... class TurbogethPayload(Payload):
    ...     chain: str = prop(PROP_TURBOGETH_CHAIN, default="rinkeby")
    ...     runtime: str = constraint(inf.INF_RUNTIME_NAME, default=TURBOGETH_RUNTIME_NAME)
    ...     min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
    ...     min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)
    ...
    >>>
    >>> async def main():
    ...     builder = DemandBuilder()
    ...     payload = TurbogethPayload(chain="mainnet", min_mem_gib=32)
    ...     await builder.decorate(payload)
    ...     print(builder)
    ...
    >>> asyncio.run(main())
    {'properties': {'golem.srv.app.eth.chain': 'mainnet'}, 'constraints': ['(&(golem.runtime.name=turbogeth-managed)\n\t(golem.inf.mem.gib>=32)\n\t(golem.inf.storage.gib>=1024))']}
    ```
    """
