import enum
from datetime import datetime
from typing import List
from ..rest.market import Market, Subscription

from dataclasses import asdict

from . import Model

# TODO in 0.4+: `cons` is not obvious, should be named `constraints`
# TODO in 0.4+: maybe `props` should just be named `properties` (?)


class DemandBuilder:
    """Builds a dictionary of properties and constraints from high-level models.

    The dictionary represents a Demand object, which is later matched by the new Golem's
    market implementation against Offers coming from providers to find those providers
    who can satisfy the requestor's demand.

    example usage:

    ```python
    >>> import yapapi
    >>> from yapapi import props as yp
    >>> from yapapi.props.builder import DemandBuilder
    >>> from datetime import datetime, timezone
    >>> builder = DemandBuilder()
    >>> builder.add(yp.Identification(name="a node", subnet_tag="testnet"))
    >>> builder.add(yp.Activity(expiration=datetime.now(timezone.utc)))
    >>> builder.__repr__
    >>> print(builder)
    {'props':
        {'golem.node.id.name': 'a node',
         'golem.node.debug.subnet': 'testnet',
         'golem.srv.comp.expiration': 1601655628772},
     'constraints': []}
    ```
    """

    def __init__(self):
        self._props: dict = {}
        self._constraints: List[str] = []
        pass

    def __repr__(self):
        return repr({"props": self._props, "constraints": self._constraints})

    @property
    def props(self) -> dict:
        """List of properties for this demand."""
        return self._props

    @property
    def cons(self) -> str:
        """List of constraints for this demand."""
        c_list = self._constraints
        c_value: str
        if not c_list:
            c_value = "()"
        elif len(c_list) == 1:
            c_value = c_list[0]
        else:
            rules = "\n\t".join(c_list)
            c_value = f"(&{rules})"

        return c_value

    def ensure(self, constraint: str):
        """Add a constraint to the demand definition."""
        self._constraints.append(constraint)

    def add(self, m: Model):
        """Add properties from the specified model to this demand definition."""
        kv = m.keys()
        base = asdict(m)

        for name in kv.names():
            prop_id = kv.__dict__[name]
            value = base[name]
            if value is None:
                continue
            if isinstance(value, datetime):
                value = int(value.timestamp() * 1000)
            if isinstance(value, enum.Enum):
                value = value.value
            assert isinstance(value, (str, int, list))
            self._props[prop_id] = value

    async def subscribe(self, market: Market) -> Subscription:
        """Create a Demand on the market and subscribe to Offers that will match that Demand."""
        return await market.subscribe(self._props, self.cons)
