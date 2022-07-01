class ObjectNotFound(Exception):
    def __init__(self, name, id_):
        self.name = name
        self.id = id_

        msg = f"{name}({id_}) doesn't exist"
        super().__init__(msg)
