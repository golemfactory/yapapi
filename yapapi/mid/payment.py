from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, TYPE_CHECKING

from ya_payment import RequestorApi, models as ya_models

from .api_call_wrapper import api_call_wrapper
from .exceptions import NoMatchingAccount
from .resource import Resource

if TYPE_CHECKING:
    from .golem_node import GolemNode


async def matching_accounts(node: "GolemNode") -> Tuple[ya_models.Account]:
    api = RequestorApi(node._ya_payment_api)
    for account in await api.get_requestor_accounts():
        if (
            account.driver.lower() == node.payment_driver
            and account.network.lower() == node.payment_network
            and account.send  # TODO: this is not checked in yapapi now? why?
        ):
            yield account


class PaymentApiResource(Resource, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_payment_api)


class Allocation(PaymentApiResource):
    @api_call_wrapper
    async def release(self) -> None:
        await self.api.release_allocation(self.id)

    @classmethod
    async def create_any_account(cls, node: "GolemNode", amount: float) -> "Allocation":
        try:
            account = await matching_accounts(node).__anext__()
        except StopAsyncIteration:
            raise NoMatchingAccount(node)

        return await cls.create_with_account(node, account, amount)

    @classmethod
    async def create_with_account(
        cls,
        node: "GolemNode",
        account: ya_models.Account,
        amount: float,
    ) -> "Allocation":
        timestamp = datetime.now(timezone.utc)
        timeout = timestamp + timedelta(days=365 * 10)

        data = ya_models.Allocation(
            address=account.address,
            payment_platform=account.platform,
            total_amount=str(amount),
            timestamp=timestamp,
            timeout=timeout,

            #   TODO: what is this deposit thing?
            make_deposit=False,

            #   TODO: why do we have to set this here?
            allocation_id="",
            spent_amount="",
            remaining_amount="",
        )

        return await cls.create(node, data)

    @classmethod
    async def create(cls, node: "GolemNode", data: ya_models.Allocation) -> "Allocation":
        api = cls._get_api(node)
        created = await api.create_allocation(data)
        return cls(node, created.allocation_id, created)

    @api_call_wrapper
    async def demand_properties_constraints(self) -> Tuple[List[Dict[str, str]], List[str]]:
        data = await self.api.get_demand_decorations([self.id])
        return data.properties, data.constraints
