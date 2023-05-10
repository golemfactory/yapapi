import pytest
from dataclasses import dataclass

from yapapi.props import constraint, prop
from yapapi.props.builder import AutodecoratingModel, DemandBuilder


@pytest.mark.asyncio
async def test_autodecorating_model():
    @dataclass
    class Foo(AutodecoratingModel):
        bar: str = prop("some.bar")
        max_baz: int = constraint("baz", "<=", 100)

    foo = Foo(bar="a nice one", max_baz=50)
    demand = DemandBuilder()
    await foo.decorate_demand(demand)
    assert demand.properties == {"some.bar": "a nice one"}
    assert demand.constraints == "(baz<=50)"


def test_add_properties():
    demand = DemandBuilder()
    assert demand.properties == {}
    demand.add_properties({"golem.foo": 667})
    demand.add_properties({"golem.bar": "blah"})
    assert demand.properties == {
        "golem.foo": 667,
        "golem.bar": "blah",
    }
