class ResourceNotFound(Exception):
    def __init__(self, name: str, id_: str):
        self.name = name
        self.id = id_

        msg = f"{name}({id_}) doesn't exist"
        super().__init__(msg)


class NoMatchingAccount(Exception):
    def __init__(self, network: str, driver: str):
        #   TODO: do we care about this sort of compatibility?
        from yapapi.engine import NoPaymentAccountError
        msg = str(NoPaymentAccountError(driver, network))
        super().__init__(msg)
