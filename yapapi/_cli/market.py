from asyncio import sleep
from datetime import timedelta, timezone, datetime
from typing import Optional

from rich.console import Console
from rich.progress import Progress

from yapapi.props import Activity
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market
from .run import async_run


def render_demand(props: dict, cons: str):
    from rich.panel import Panel
    from rich.columns import Columns

    format_items = "\n\n".join(
        f"[b cyan]{key}:[/b cyan]\n{repr(value)}" for key, value in props.items()
    )
    props_panel = Panel(f"[bold]Props:[/bold]\n\n{format_items}")
    cons_panel = Panel(f"[bold]Cons:[/bold]\n\n{cons}", expand=True)
    return Columns([props_panel, cons_panel], equal=True)


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

    @async_run
    async def vm(
        self,
        image_hash: str,
        min_mem_gib: float = 0.5,
        min_storage_gib: float = 2.0,
        timeout=timedelta(minutes=5),
    ):
        from yapapi.runner import vm

        console = Console()
        now = datetime.now(timezone.utc)
        if isinstance(timeout, int):
            timeout = timedelta(minutes=timeout)
        assert isinstance(timeout, timedelta)
        expires = now + timeout
        pacakge = await vm.repo(
            image_hash=image_hash, min_mem_gib=min_mem_gib, min_storage_gib=min_storage_gib
        )
        package_url = await pacakge.resolve_url()

        console.print(f"resolved vm package: {package_url}")
        console.print(f"demand expires: {expires}")
        demand = DemandBuilder()
        demand.add(Activity(expiration=expires))
        await pacakge.decorate_demand(demand)
        console.print(render_demand(demand.props, demand.cons))
