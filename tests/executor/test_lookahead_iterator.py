"""Unit tests for the class `yapapi.executor.utils.LookaheadIterator`."""
import pytest

from yapapi.executor.utils import LookaheadIterator


def test_empty_is_empty():
    it = LookaheadIterator([])
    assert it.empty
    assert it.empty


def test_empty_no_first():
    it = LookaheadIterator([])
    with pytest.raises(StopIteration):
        _ = it.first
    assert it.empty


def test_empty_no_next():
    it = LookaheadIterator([])
    with pytest.raises(StopIteration):
        _ = next(it)
    assert it.empty


def test_empty_iterable():
    it = LookaheadIterator([])
    for _ in it:
        assert False
    assert it.empty


def test_singleton_not_empty():
    it = LookaheadIterator([1])
    assert not it.empty
    assert not it.empty
    assert it.first == 1
    assert not it.empty
    assert it.first == 1


def test_singleton_has_next():
    it = LookaheadIterator([1])
    assert next(it) == 1
    assert it.empty


def _all_lists_shorter_than(len):
    from itertools import permutations

    for l in range(len):
        for p in permutations(range(l), l):
            yield list(p)


def test_iteration():
    for input in _all_lists_shorter_than(5):
        it = LookaheadIterator(input)
        output = list(it)
        assert input == output, f"input = {input}, output = {output}"


def test_iteration_first():
    for input in _all_lists_shorter_than(5):
        it = LookaheadIterator(input)
        if it.empty:
            assert input == []
        else:
            output = [it.first]
            for _ in it:
                if not it.empty:
                    output.append(it.first)
            assert input == output, f"input = {input}, output = {output}"
