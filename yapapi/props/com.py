from typing import Dict, Any
import enum
from dataclasses import dataclass, field
from .base import Model, Props, as_list

SCHEME: str = "golem.com.scheme"
PRICE_MODEL: str = "golem.com.pricing.model"

LINEAR_COEFFS: str = "golem.com.pricing.model.linear.coeffs"
DEFINED_USAGES: str = "golem.com.usage.vector"


class BillingScheme(enum.Enum):
    PAYU = "payu"


class PriceModel(enum.Enum):
    LINEAR = "linear"


class Counter(enum.Enum):
    TIME = "golem.usage.duration_sec"
    CPU = "golem.usage.cpu_sec"
    STORAGE = "golem.usage.storage_gib"
    MAXMEM = "golem.usage.gib"
    UNKNOWN = ""


@dataclass(frozen=True)
class Com(Model):
    scheme: BillingScheme = field(metadata={"key": SCHEME})
    price_model: PriceModel = field(metadata={"key": PRICE_MODEL})


@dataclass(frozen=True)
class ComLinear(Com):
    fixed_price: float
    price_for: Dict[Counter, float]

    @classmethod
    def _custom_mapping(cls, props: Props, data: Dict[str, Any]):
        assert data["price_model"] == PriceModel.LINEAR, "expected linear pricing model"

        coeffs = as_list(props[LINEAR_COEFFS])
        usages = as_list(props[DEFINED_USAGES])

        fixed_price = float(coeffs.pop())
        price_for = ((Counter(usages[i]), float(coeffs[i])) for i in range(len(coeffs)))

        data.update(fixed_price=fixed_price, price_for=dict(price_for))
