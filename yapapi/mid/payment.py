from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, TYPE_CHECKING
from decimal import Decimal

from ya_payment import RequestorApi, models as ya_models

from .api_call_wrapper import api_call_wrapper
from .exceptions import NoMatchingAccount
from .resource import Resource

if TYPE_CHECKING:
    from .golem_node import GolemNode


class PaymentApiResource(Resource, ABC):
    @property
    def api(self) -> RequestorApi:
        return RequestorApi(self._node._ya_payment_api)


class Allocation(PaymentApiResource):
    @api_call_wrapper(ignore=[404, 410])
    async def release(self) -> None:
        await self.api.release_allocation(self.id)

    @classmethod
    async def create_any_account(
        cls, node: "GolemNode", amount: Decimal, network: str, driver: str,
    ) -> "Allocation":
        for account in await cls._get_api(node).get_requestor_accounts():
            if (
                account.driver.lower() == driver.lower()
                and account.network.lower() == network.lower()
            ):
                break
        else:
            raise NoMatchingAccount(network, driver)

        return await cls.create_with_account(node, account, amount)

    @classmethod
    async def create_with_account(
        cls,
        node: "GolemNode",
        account: ya_models.Account,
        amount: Decimal,
    ) -> "Allocation":
        timestamp = datetime.now(timezone.utc)
        timeout = timestamp + timedelta(days=365 * 10)

        data = ya_models.Allocation(
            address=account.address,
            payment_platform=account.platform,
            total_amount=str(amount),
            timestamp=timestamp,
            timeout=timeout,

            #   This will probably be removed one day (consent-related thing)
            make_deposit=False,

            #   We must set this here because of the ya_client interface
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

    @api_call_wrapper()
    async def demand_properties_constraints(self) -> Tuple[List[Dict[str, str]], List[str]]:
        data = await self.api.get_demand_decorations([self.id])
        return data.properties, data.constraints
