"""Payment-related properties."""
import abc
from typing import Dict, Any, List
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


@dataclass(frozen=True)  # type: ignore  # mypy doesn't allow abstract methods in dataclasses
class Com(Model):
    """Base model representing the payment model used."""

    scheme: BillingScheme = field(metadata={"key": SCHEME})
    price_model: PriceModel = field(metadata={"key": PRICE_MODEL})

    @abc.abstractmethod
    def calculate_cost(self, usage: List) -> float:
        """Calculate the cost by applying the provided usage vector to the underlying pricing model."""

    @abc.abstractmethod
    def usage_as_dict(self, usage: List) -> Dict:
        """Return usage as a dictionary where keys are the appropriate usage counters."""


@dataclass(frozen=True)
class ComLinear(Com):
    """Linear payment model."""

    linear_coeffs: List[float] = field(metadata={"key": LINEAR_COEFFS})
    usage_vector: List[str] = field(metadata={"key": DEFINED_USAGES})

    @classmethod
    def _custom_mapping(cls, props: Props, data: Dict[str, Any]):
        # we don't need mapping per-se but we'll do some validation instead
        assert data["price_model"] == PriceModel.LINEAR, "expected linear pricing model"
        assert (
            len(data["linear_coeffs"]) == len(data["usage_vector"]) + 1
        ), "expecting the number of linear_coeffs to correspond to usage_vector + 1 (fixed price)"
        assert all(
            [isinstance(lc, float) for lc in data["linear_coeffs"]]
        ), "linear_coeffs values must be `float`"
        assert all(
            [isinstance(u, str) for u in data["usage_vector"]]
        ), "usage_vector values must be `str`"

    @property
    def fixed_price(self) -> float:
        return self.linear_coeffs[-1]

    @property
    def price_for(self) -> Dict[str, float]:
        return {u: self.linear_coeffs[i] for (i, u) in enumerate(self.usage_vector)}

    def calculate_cost(self, usage: List):
        usage = usage + [1.0]  # append the "usage" of the fixed component
        return sum([c * usage[i] for (i, c) in enumerate(self.linear_coeffs)])

    def usage_as_dict(self, usage: List) -> Dict[str, float]:
        return {self.usage_vector[i]: u for (i, u) in enumerate(usage)}
