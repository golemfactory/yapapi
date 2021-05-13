from dataclasses import dataclass
import pytest

from yapapi.props import prop, constraint
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
    assert demand.constraints == "(((baz<=50)))"
