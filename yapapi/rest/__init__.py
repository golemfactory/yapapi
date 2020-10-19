"""
Mid-level binding for Golem REST API.

Serves as a more convenient interface between the agent code and the REST API.
"""

from . import activity
from . import market

from .configuration import Configuration
from .market import Market
from .payment import Payment
from .activity import ActivityService as Activity


__all__ = ("Configuration", "Market", "Payment", "Activity")
