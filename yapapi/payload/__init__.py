import abc

from yapapi.props.builder import DemandDecorator


class Payload(DemandDecorator, abc.ABC):
    """Base class for descriptions of the payload required by the requestor."""
