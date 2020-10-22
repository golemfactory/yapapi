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
            "Missing key: 'golem.com.pricing.model.linear.coeffs'",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
            },
            "pop from empty list",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": [0.001, 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec"],
                "golem.com.scheme": "payu",
            },
            "list index out of range",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": ["spam", 0.002, 0.0],
                "golem.com.usage.vector": ["golem.usage.cpu_sec", "golem.usage.duration_sec"],
                "golem.com.scheme": "payu",
            },
            "could not convert string to float",
        ),
        (
            {
                "golem.com.pricing.model": "linear",
                "golem.com.pricing.model.linear.coeffs": ["spam", 0.002, 0.0],
                "golem.com.usage.vector": "not a vector",
                "golem.com.scheme": "payu",
            },
            "Error when decoding 'not a vector'",
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
