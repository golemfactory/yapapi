import abc

from yapapi.props.builder import AutodecoratingModel


class Payload(AutodecoratingModel, abc.ABC):
    """Base class for descriptions of the payload required by the requestor.

    example usage::

        import asyncio

        from dataclasses import dataclass
        from yapapi.props.builder import DemandBuilder
        from yapapi.props.base import prop, constraint
        from yapapi.props import inf

        from yapapi.payload import Payload

        CUSTOM_RUNTIME_NAME = "my-runtime"
        CUSTOM_PROPERTY = "golem.srv.app.myprop"


        @dataclass
        class MyPayload(Payload):
            myprop: str = prop(CUSTOM_PROPERTY, default="myvalue")
            runtime: str = constraint(inf.INF_RUNTIME_NAME, default=CUSTOM_RUNTIME_NAME)
            min_mem_gib: float = constraint(inf.INF_MEM, operator=">=", default=16)
            min_storage_gib: float = constraint(inf.INF_STORAGE, operator=">=", default=1024)


        async def main():
            builder = DemandBuilder()
            payload = MyPayload(myprop="othervalue", min_mem_gib=32)
            await builder.decorate(payload)
            print(builder)

        asyncio.run(main())

    output::

        {'properties': {'golem.srv.app.myprop': 'othervalue'}, 'constraints': ['(&(golem.runtime.name=my-runtime)\n\t(golem.inf.mem.gib>=32)\n\t(golem.inf.storage.gib>=1024))']}
    """
