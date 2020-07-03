from .market import Demand
from .payment import Allocation, Invoices


class Cli:
    def __init__(self):
        self.allocation = Allocation
        self.invoice = Invoices
        self.demand = Demand


def _main():
    import fire  # type: ignore

    fire.Fire(Cli, name="yapapi")
