from typing import AsyncContextManager, TypeVar

_T = TypeVar("_T")


class ResourceCtx(AsyncContextManager[_T]):
    async def detach(self) -> _T:
        resource = await self.__aenter__()
        return resource
