import pytest
from yapapi.props.com import ComLinear, Counter
from tests.factories.props.com import ComLinearFactory

LINEAR_COEFFS = [0.1, 0.2, 0.3]
DEFINED_USAGES = [Counter.CPU.value, Counter.TIME.value]


def test_com_linear_fixed_price():
    com: ComLinear = ComLinearFactory(linear_coeffs=LINEAR_COEFFS)
    assert com.fixed_price == LINEAR_COEFFS[-1]


def test_com_linear_price_for():
    com: ComLinear = ComLinearFactory(
        linear_coeffs=LINEAR_COEFFS,
        usage_vector=DEFINED_USAGES
    )
    assert com.price_for[Counter.CPU] == LINEAR_COEFFS[0]
    assert com.price_for[Counter.TIME] == LINEAR_COEFFS[1]


@pytest.mark.parametrize(
    "usage, cost",
    [
        ([0.0, 0.0], LINEAR_COEFFS[-1],),
        ([2.0, 0.0], LINEAR_COEFFS[0] * 2.0 + LINEAR_COEFFS[-1],),
        ([0.0, 2.0], LINEAR_COEFFS[1] * 2.0 + LINEAR_COEFFS[-1],),
        ([3.0, 5.0], LINEAR_COEFFS[0] * 3.0 + LINEAR_COEFFS[1] * 5.0 + LINEAR_COEFFS[-1],),
    ]
)
def test_com_linear_calculate_cost(usage, cost):
    com: ComLinear = ComLinearFactory(
        linear_coeffs=LINEAR_COEFFS,
        usage_vector=DEFINED_USAGES
    )
    assert com.calculate_cost(usage) == cost

