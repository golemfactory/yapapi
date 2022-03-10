import pytest

from yapapi.strategy import (
    MarketStrategy,
    PROP_DEBIT_NOTE_ACCEPTANCE_TIMEOUT,
    PROP_DEBIT_NOTE_INTERVAL_SEC,
    PROP_PAYMENT_TIMEOUT_SEC,
    PropValueRange,
)
from yapapi.strategy.base import DEFAULT_PROPERTY_VALUE_RANGES


class BadStrategy(MarketStrategy):  # noqa
    pass


class GoodStrategy(MarketStrategy):
    async def score_offer(self, offer) -> float:
        pass


class PropValueOverrideStrategy(GoodStrategy):
    acceptable_prop_value_range_overrides = {
        PROP_DEBIT_NOTE_INTERVAL_SEC: PropValueRange(120, 1200)
    }


def test_bad_strategy_instantiate():
    with pytest.raises(TypeError) as e:
        BadStrategy()

    assert str(e.value).startswith("Can't instantiate abstract class BadStrategy")


def test_good_strategy_instantiate():
    GoodStrategy()


def test_acceptable_prop_value_ranges_defaults():
    strategy = GoodStrategy()
    assert strategy.acceptable_prop_value_ranges == DEFAULT_PROPERTY_VALUE_RANGES
    assert strategy.acceptable_prop_value_ranges is not DEFAULT_PROPERTY_VALUE_RANGES


def test_acceptable_prop_value_range_override():
    defaults = DEFAULT_PROPERTY_VALUE_RANGES
    overrides = PropValueOverrideStrategy.acceptable_prop_value_range_overrides
    effective_ranges = PropValueOverrideStrategy().acceptable_prop_value_ranges

    # sanity check
    assert overrides[PROP_DEBIT_NOTE_INTERVAL_SEC] != defaults[PROP_DEBIT_NOTE_INTERVAL_SEC]

    assert effective_ranges[PROP_DEBIT_NOTE_INTERVAL_SEC] == overrides[PROP_DEBIT_NOTE_INTERVAL_SEC]
    assert effective_ranges[PROP_PAYMENT_TIMEOUT_SEC] == defaults[PROP_PAYMENT_TIMEOUT_SEC]
    assert (
        effective_ranges[PROP_DEBIT_NOTE_ACCEPTANCE_TIMEOUT]
        == defaults[PROP_DEBIT_NOTE_ACCEPTANCE_TIMEOUT]
    )


@pytest.mark.parametrize(
    "min, max, val, contains, clamped, clamp_error",
    [
        (None, None, 0, True, 0, False),
        (None, None, 12345, True, 12345, False),
        (None, None, -0.12345, True, -0.12345, False),
        (-42, None, 0, True, 0, False),
        (42, None, 42, True, 42, False),
        (0, None, -66, False, 0, False),
        (None, 42, 0, True, 0, False),
        (None, -42, 0, False, -42, False),
        (None, 0, 0, True, 0, False),
        (-42.5, 66.7, 0, True, 0, False),
        (-42.5, 66.7, -42.5, True, -42.5, False),
        (-42.5, 66.7, 66.7, True, 66.7, False),
        (-42.5, 66.7, -66, False, -42.5, False),
        (-42.5, 66.7, 88, False, 66.7, False),
        (256, 0, 128, False, None, True),
    ],
)
def test_prop_value_range(min, max, val, contains, clamped, clamp_error):
    range = PropValueRange(min, max)
    assert (val in range) == contains
    if clamp_error:
        with pytest.raises(ValueError):
            range.clamp(val)
    else:
        assert range.clamp(val) == clamped
