from yapapi.rest import Configuration, Payment, payment
from rich.console import Console
from rich.table import Table
from rich.progress import track
from typing import Optional
from decimal import Decimal
from .run import async_run
from asyncio import sleep

INVOICE_STATUS_COLOR = {
    payment.InvoiceStatus.RECEIVED: "",
    payment.InvoiceStatus.REJECTED: "bold red",
    payment.InvoiceStatus.FAILED: "red",
    payment.InvoiceStatus.ISSUED: "yellow",
    payment.InvoiceStatus.ACCEPTED: "yellow",
    payment.InvoiceStatus.SETTLED: "green",
}


class Allocation:
    """Payment allocation managment.

    """

    def __init__(self, appkey: Optional[str] = None):
        self._cli = Configuration(app_key=appkey)

    @async_run
    async def new(self, amount: str):
        console = Console()
        async with self._cli.payment() as p:
            p = Payment(p)
            free_allocation = await p.new_allocation(amount=Decimal(amount)).detach()
            console.print(free_allocation)

    @async_run
    async def list(self, details: bool = False):
        "Lists current active payment allocation"

        console = Console()
        console.print(f"[bold]details[/bold]: {details}")
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Id")
        table.add_column("Amount")
        table.add_column("Expires")

        async with self._cli.payment() as p:
            p = Payment(p)
            async for allocation in p.allocations():
                table.add_row(
                    allocation.id,
                    str(allocation.amount),
                    str(allocation.expires) if allocation.expires else None,
                )

            if table.row_count:
                console.print(table)
            else:
                console.print("[bold]No allocations[/bold]")

    @async_run
    async def clear(self):
        "Removes all active payment allocations"

        console = Console()
        async with self._cli.payment() as client:
            p = Payment(client)
            allocations = [a async for a in p.allocations()]
            for allocation in track(allocations, description="cleaning allocations"):
                await allocation.delete()
                await sleep(0.2)
        console.print(f"{len(allocations)} removed")


class Invoices:
    """Invoice managment.

    """

    def __init__(self, appkey: Optional[str] = None):
        self._cli = Configuration(app_key=appkey)

    @async_run
    async def incoming(self):
        console = Console()
        async with self._cli.payment() as client:
            p = Payment(client)
            async for invoice in p.incoming_invoices():
                console.print(invoice)
        console.print("done")

    @async_run
    async def accept(self, allocation_id: str, invoice_id: str):
        """
        Accepts given `invoice_id` with `allocation_id`

        :param allocation_id: Allocation from which invoice will be paid. see
            `allocation list`.
        :param invoice_id: Invoice identifier.
        """
        console = Console()
        async with self._cli.payment() as client:
            p = Payment(client)
            invoice = await p.invoice(invoice_id)
            allocation = await p.allocation(allocation_id)
            await invoice.accept(amount=invoice.amount, allocation=allocation)
        console.print("done")

    @async_run
    async def list(self, by_status: Optional[str] = None):
        """
        Lists all invoices.
        """
        from rich.columns import Columns
        from rich.text import Text

        def status_to_color(status: payment.InvoiceStatus) -> Optional[str]:
            return INVOICE_STATUS_COLOR.get(str(status), None)

        def filter_invoice(invoice: payment.Invoice) -> bool:
            if by_status:
                return str(invoice.status) == by_status
            return True

        def format_invoice(invoice: payment.Invoice):
            status_style = status_to_color(invoice.status) or ""
            table = Table(
                "[yellow]Attribute[/yellow]",
                "[yellow]Value[/yellow]",
                header_style="bold yellow",
                title=invoice.invoice_id,
                style=status_style,
                title_style=status_style,
            )
            table.add_row(Text.assemble("issuer", style="bold"), invoice.issuer_id)
            date_txt = str(invoice.timestamp.date())
            time_txt = str(invoice.timestamp.time())
            table.add_row("ts", f"{date_txt} [dim]{time_txt}[/dim]")

            table.add_row("amount", invoice.amount)
            table.add_row("status", Text.assemble(str(invoice.status), style=status_style))
            return table

        console = Console()
        async with self._cli.payment() as client:
            p = Payment(client)
            console.print(
                Columns(
                    [
                        format_invoice(invoice)
                        async for invoice in p.invoices()
                        if filter_invoice(invoice)
                    ],
                    width=60,
                    padding=(2, 1),
                )
            )
