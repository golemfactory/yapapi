from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import GolemNode


class ObjectNotFound(Exception):
    def __init__(self, name, id_):
        self.name = name
        self.id = id_

        msg = f"{name}({id_}) doesn't exist"
        super().__init__(msg)


class NoMatchingAccount(Exception):
    def __init__(self, node: "GolemNode"):
        self.node = node

        #   TODO: do we care about this sort of compatibility?
        from yapapi.engine import NoPaymentAccountError
        msg = str(NoPaymentAccountError(node.payment_driver, node.payment_network))
        super().__init__(msg)
