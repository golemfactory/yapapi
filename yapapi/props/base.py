from typing import Dict, Type, Any, Union, List, cast
import abc, enum, json
from dataclasses import dataclass, fields, MISSING

Props = Dict[str, str]


def as_list(data: Union[str, List[str]]) -> List[str]:
    if not isinstance(data, str):
        return list(data)
    item = json.loads(data)
    if isinstance(item, list):
        return cast(List[str], item)
    return [str(item)]


def _find_enum(enum_type: Type[enum.Enum], val: str) -> Any:
    for member in enum_type.__members__.values():
        if member.value == val:
            return member
    return None


@dataclass(frozen=True)
class _PyField:
    name: str
    type: type
    required: bool

    def encode(self, value: str):
        if issubclass(self.type, enum.Enum):
            return self.name, self.type(value)
        return self.name, value


class Model(abc.ABC):
    def __init__(self, **kwargs):
        pass

    @classmethod
    def _custom_mapping(cls, props: Props, data: Dict[str, Any]):
        pass

    @classmethod
    def from_props(cls, props: Props) -> "Model":
        field_map = dict(
            (
                f.metadata["key"],
                _PyField(name=f.name, type=f.type, required=f.default is MISSING),
            )
            for f in fields(cls)
            if "key" in f.metadata
        )
        data = dict(
            (
                field_map[key].encode(val)
                for (key, val) in props.items()
                if key in field_map
            )
        )
        cls._custom_mapping(props, data)
        print(dict(data))
        self = cls(**data)
        return self

    @classmethod
    def keys(cls):
        class _Keys:
            def __init__(self, iter):
                self.__dict__ = dict(iter)

        return _Keys((f.name, f.metadata["key"]) for f in fields(cls))


__all__ = ("Model", "as_list", "Props")
