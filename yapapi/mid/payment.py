from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, TYPE_CHECKING

from ya_payment import RequestorApi, models as ya_models

from .golem_object import GolemObject
from .exceptions import NoMatchingAccount

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


class PaymentApiObject(GolemObject, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_payment_api)

    @property
    def model(self):
        my_name = type(self).__name__
        return ya_models.getattr(my_name)


class Allocation(PaymentApiObject):
    @property
    def _delete_method_name(self) -> str:
        return 'release_allocation'

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

        model = ya_models.Allocation(
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

        return await cls.create(node, model)

    @classmethod
    async def create(cls, node: "GolemNode", model: ya_models.Allocation) -> "Allocation":
        api = cls._get_api(node)
        created = await api.create_allocation(model)
        return cls(node, created.allocation_id, created)

    async def demand_properties_constraints(self) -> Tuple[List[Dict[str, str]], List[str]]:
        data = await self.api.get_demand_decorations([self.id])
        return data.properties, data.constraints
