"""Payment-related properties."""
from typing import Dict, Any
import enum
from dataclasses import dataclass, field
from .base import Model, Props, as_list

SCHEME: str = "golem.com.scheme"
"""the key which defines the billing scheme"""
PRICE_MODEL: str = "golem.com.pricing.model"
"""the key which defines the pricing model"""

LINEAR_COEFFS: str = "golem.com.pricing.model.linear.coeffs"
"""the key which defines the pricing model's linear coefficients"""
DEFINED_USAGES: str = "golem.com.usage.vector"
"""the key which defines the usages vector"""


class BillingScheme(enum.Enum):
    """Enum of possible billing schemes."""

    PAYU = "payu"
    """payment for usage"""


class PriceModel(enum.Enum):
    """Enum of possible pricing models."""

    LINEAR = "linear"
    """the cost depends linearly on the value it derives from"""


class Counter(enum.Enum):
    """Enum of possible resources, the usage of which is paid for."""

    TIME = "golem.usage.duration_sec"
    """execution time"""
    CPU = "golem.usage.cpu_sec"
    """cpu time"""
    STORAGE = "golem.usage.storage_gib"
    """disk storage"""
    MAXMEM = "golem.usage.gib"
    """maximum memory usage"""
    UNKNOWN = ""
    """unknown resource"""


@dataclass(frozen=True)
class Com(Model):
    """Base model for properties describing various payment models."""

    scheme: BillingScheme = field(metadata={"key": SCHEME})
    price_model: PriceModel = field(metadata={"key": PRICE_MODEL})


@dataclass(frozen=True)
class ComLinear(Com):
    """Payment model with linear pricing."""

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
