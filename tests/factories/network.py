import asyncio
from concurrent import futures
import factory
import faker
from unittest import mock

from yapapi.network import Network


class NetworkFactory(factory.Factory):
    class Meta:
        model = Network

    ip = factory.Faker("ipv4", network=True)
    owner_id = factory.LazyFunction(lambda: "0x" + faker.Faker().binary(length=20).hex())

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        if "net_api" not in kwargs:
            net_api = mock.AsyncMock()
            net_api.create_network = mock.AsyncMock(
                return_value=faker.Faker().binary(length=16).hex()
            )
            net_api.remove_network = mock.AsyncMock()
            kwargs["net_api"] = net_api

        # we're using `futures.ThreadPoolExecutor` here
        # to run an async awaitable in a synchronous manner
        pool = futures.ThreadPoolExecutor()
        return pool.submit(asyncio.run, model_class.create(*args, **kwargs)).result()
