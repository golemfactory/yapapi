from typing import Optional

from rich.console import Console
from rich.columns import Columns
from rich.progress import Progress
from yapapi.rest import Configuration, Market
from asyncio import sleep
from .run import async_run


class Demand:
    """Offer subscription managment.

    """

    def __init__(self, appkey: Optional[str] = None):
        self._cli = Configuration(app_key=appkey)
        self.create = Create

    @async_run
    async def list(self, only_expired: bool = False):
        """Lists all active demands"""
        console = Console()
        async with self._cli.market() as client:
            market_api = Market(client)
            it: int = 0
            async for demand in market_api.subscriptions():
                it += 1
                console.print(demand.details.to_dict().items())
                console.print()
            console.print(f"{it} demands running")

    @async_run
    async def clear(self, only_expired: bool = False):
        """Removes demands.

        By default removes all demands.

        :param only_expired: removes only expired demands.

        """
        console = Console()
        it = 0
        with Progress(console=console) as progress:
            tid = progress.add_task("dropping demands", start=False)
            async with self._cli.market() as client:
                market_api = Market(client)
                async for demand in market_api.subscriptions():
                    await demand.delete()
                    it += 1
                    progress.advance(tid)
                    await sleep(0.2)
            progress.update(tid, completed=True)
        console.print(f"{it} demands removed")


class Create:
    def wasm(self, url: str, *, ram: float = 0.5, storage: float = 1.0):

        pass
