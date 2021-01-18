import dataclasses


def dataclass_fields_dict(data_class):
    return {field.name: field for field in dataclasses.fields(data_class)}
