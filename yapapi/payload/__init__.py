import abc

from yapapi.props.builder import AutodecoratingModel


class Payload(AutodecoratingModel, abc.ABC):
    """Base class for descriptions of the payload required by the requestor."""
