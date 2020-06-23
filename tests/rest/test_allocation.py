import pytest
from yapapi.rest import Configuration, Payment
from decimal import Decimal


@pytest.fixture
async def yapapi_payment(request):
    conf = Configuration(app_key=request.config.getvalue("yaApiKey"))
    async with conf.payment() as p:
        yield Payment(p)


@pytest.mark.skipif("not config.getvalue('yaApiKey')")
@pytest.mark.asyncio
async def test_allocation(yapapi_payment: Payment):

    async for a in yapapi_payment.allocations():
        print("a=", a)

    async with yapapi_payment.new_allocation(amount=Decimal(40)) as allocation:
        found = False
        async for a in yapapi_payment.allocations():
            if a.id == allocation.id:
                found = True
                break
        assert found
