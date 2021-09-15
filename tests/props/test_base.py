import pytest

from dataclasses import dataclass
from yapapi.props import base as props_base


@dataclass
class Foo(props_base.Model):
    bar: str = props_base.prop("bar", "cafebiba")
    max_baz: int = props_base.constraint("baz", "<=", 100)
    min_baz: int = props_base.constraint("baz", ">=", 1)
    lst: list = props_base.constraint("lst", "=", default_factory=list)


@dataclass
class FooToo(props_base.Model):
    baz: int = props_base.constraint("baz", "=", 21)


@dataclass
class FooZero(props_base.Model):
    pass


def test_constraint_fields():
    fields = Foo.constraint_fields()
    assert len(fields) == 3
    assert any(f.name == "max_baz" for f in fields)
    assert all(f.name != "bar" for f in fields)


def test_property_fields():
    fields = Foo.property_fields()
    assert len(fields) == 1
    assert any(f.name == "bar" for f in fields)
    assert all(f.name != "max_baz" for f in fields)


def test_constraint_to_str():
    foo = Foo(max_baz=42)
    max_baz = [f for f in foo.constraint_fields() if f.name == "max_baz"][0]
    assert props_base.constraint_to_str(foo.max_baz, max_baz) == "(baz<=42)"


@pytest.mark.parametrize(
    "value, constraint_str", [
        (["one"], "(lst=one)"),
        (["one", "two"], "(&(lst=one)\n\t(lst=two))"),
    ]
)
def test_constraint_to_str_list(value, constraint_str):
    foo = Foo(lst=value)
    lst = [f for f in foo.constraint_fields() if f.name == "lst"][0]
    assert props_base.constraint_to_str(foo.lst, lst) == constraint_str


def test_constraint_model_serialize():
    foo = Foo()
    constraints = props_base.constraint_model_serialize(foo)
    assert constraints == ["(baz<=100)", "(baz>=1)", ""]


@pytest.mark.parametrize(
    "model, operator, result, error",
    [
        (
            Foo(),
            None,
            "(&(baz<=100)\n\t(baz>=1))",
            False,
        ),
        (
            Foo(),
            "&",
            "(&(baz<=100)\n\t(baz>=1))",
            False,
        ),
        (
            Foo(lst=["one"]),
            None,
            "(&(baz<=100)\n\t(baz>=1)\n\t(lst=one))",
            False,
        ),
        (
            Foo(lst=["one", "other"]),
            None,
            "(&(baz<=100)\n\t(baz>=1)\n\t(&(lst=one)\n\t(lst=other)))",
            False,
        ),
        (
            Foo(),
            "|",
            "(|(baz<=100)\n\t(baz>=1))",
            False,
        ),
        (
            Foo(),
            "!",
            None,
            True,
        ),
        (
            FooToo(),
            "!",
            "(!(baz=21))",
            False,
        ),
        (
            FooToo(),
            "&",
            "(baz=21)",
            False,
        ),
        (
            FooToo(),
            "|",
            "(baz=21)",
            False,
        ),
        (
            FooZero(),
            None,
            "(&)",
            False,
        ),
        (
            FooZero(),
            "&",
            "(&)",
            False,
        ),
        (
            FooZero(),
            "|",
            "(|)",
            False,
        ),
    ],
)
def test_join_str_constraints(model, operator, result, error):
    args = [props_base.constraint_model_serialize(model)]
    if operator:
        args.append(operator)
    try:
        constraints_str = props_base.join_str_constraints(*args)
        assert constraints_str == result
    except props_base.ConstraintException:
        if not error:
            raise
    else:
        assert not error, "props_base.ConstraintException not raised"
