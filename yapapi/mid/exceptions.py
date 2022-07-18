class ResourceNotFound(Exception):
    def __init__(self, name: str, id_: str):
        self.name = name
        self.id = id_

        msg = f"{name}({id_}) doesn't exist"
        super().__init__(msg)


class NoMatchingAccount(Exception):
    def __init__(self, network: str, driver: str):
        #   NOTE: we don't really care about this sort of compatibility, but this is
        #         a message developers are used to so maybe it's worth reusing
        from yapapi.engine import NoPaymentAccountError
        msg = str(NoPaymentAccountError(driver, network))
        super().__init__(msg)
