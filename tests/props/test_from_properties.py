"""Unit tests for `yapapi.props.base.Model.from_properties` method"""

import pytest

from yapapi.props import com, InvalidPropertiesError


@pytest.mark.parametrize(
    "properties, error_msg",
    [
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
            },
            None,
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
                "golem.superfluous.key": "Some other stuff",
            },
            None,
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                # "golem.com.scheme": "payu",
            },
            "missing 1 required positional argument: 'scheme'",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                # "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
            },
            "Invalid properties: Missing key: 'linear_coeffs'",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
            },
            "Invalid properties: expecting the number of linear_coeffs to correspond to usage_vector + 1 (fixed price)",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec"],
                "golem.com.scheme": "payu",
            },
            "Invalid properties: expecting the number of linear_coeffs to correspond to usage_vector + 1 (fixed price)",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": ["spam", 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
            },
            "Invalid properties: linear_coeffs values must be `float`",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": "not a vector",
                "golem.com.scheme": "payu",
            },
            "Invalid properties: expecting the number of linear_coeffs to correspond to usage_vector + 1 (fixed price)",
        ),
    ],
)
def test_from_props(properties, error_msg):
    """Check whether `from_properties(properties)` raises errors for invalid `properties`."""

    try:
        com.ComLinear.from_properties(properties)
    except InvalidPropertiesError as exc:
        if error_msg is not None:
            assert error_msg in str(exc)
        else:
            assert False, exc
    else:
        assert (
            error_msg is None
        ), f"Expected InvalidPropertiesError with msg including '{error_msg}'."
