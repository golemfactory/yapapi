import enum
from datetime import datetime
from typing import List
from ..rest.market import Market, Subscription

from dataclasses import asdict

from . import Model


class DemandBuilder:
    def __init__(self):
        self._props: dict = {}
        self._constraints: List[str] = []
        pass

    def __repr__(self):
        return repr({"props": self._props, "constraints": self._constraints})

    @property
    def props(self) -> dict:
        return self._props

    @property
    def cons(self) -> str:
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
        self._constraints.append(constraint)

    def add(self, m: Model):
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
        return await market.subscribe(self._props, self.cons)
