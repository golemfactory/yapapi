import abc

from yapapi.props.builder import DemandBuilder


class Payload(abc.ABC):
    """Base class for descriptions of the payload required by the requestor."""

    @abc.abstractmethod
    async def decorate_demand(self, demand: DemandBuilder):
        """Hook that allows the specific payload to add appropriate properties and constraints to a Demand."""
