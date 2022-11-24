"""
Mid-level binding for Golem REST API.

Serves as a more convenient interface between the agent code and the REST API.
"""

from . import activity, market
from .activity import ActivityService as Activity
from .configuration import Configuration
from .market import Market
from .net import Net
from .payment import Payment

__all__ = ("Configuration", "Market", "Payment", "Activity", "Net")
