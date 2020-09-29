"""Unit tests for `yapapi.props.base.Model.from_props` method"""

import pytest

from yapapi.props import com, InvalidPropertiesError


@pytest.mark.parametrize(
    "props, error_msg",
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
def test_from_props(props, error_msg):
    """Check whether `from_props(props)` raises errors for invalid `props`."""

    try:
        com.ComLinear.from_props(props)
    except InvalidPropertiesError as exc:
        if error_msg is not None:
            assert error_msg in str(exc)
        else:
            assert False, exc
