import abc

from yapapi.props.builder import DemandBuilder


class Package(abc.ABC):
    """Information on task package to be used for running tasks on providers."""

    @abc.abstractmethod
    async def resolve_url(self) -> str:
        """Return package URL."""

    @abc.abstractmethod
    async def decorate_demand(self, demand: DemandBuilder):
        """Add package information to a Demand."""
