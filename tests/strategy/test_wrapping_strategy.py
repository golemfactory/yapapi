from unittest import mock

from yapapi.strategy import WrappingMarketStrategy


class MyBaseStrategy(mock.Mock):
    some_value = 123
    some_other_value = 456


class MyStrategy(WrappingMarketStrategy):
    some_value = 789

    def some_method(self):
        return True


def test_wrapping_strategy():
    base_strategy = MyBaseStrategy()
    strategy = MyStrategy(base_strategy)

    assert strategy.some_value == MyStrategy.some_value
    assert strategy.some_other_value == MyBaseStrategy.some_other_value

    strategy.some_method()
    assert not base_strategy.some_method.called

    strategy.some_other_method()
    assert base_strategy.some_other_method.called
