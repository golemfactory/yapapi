from typing import Dict, Type, Any, Union, List, cast, TypeVar
import typing
import abc
import enum
import json
from dataclasses import dataclass, fields, MISSING
from datetime import datetime, timezone

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
        def get_type_origin(t):
            # type: ignore
            if hasattr(typing, "get_origin"):
                return typing.get_origin(t)  # type: ignore
            else:
                return getattr(t, "__origin__", None)

        def get_type_args(t):
            # >= py3.8
            if hasattr(typing, "get_args"):
                return typing.get_args(t)  # type: ignore
            else:
                return getattr(t, "__args__")

        # print("name=", self.name, "type=", self.type, type(self.type))
        if get_type_origin(self.type) == Union:
            if datetime in get_type_args(self.type):
                # TODO: fix this.
                return self.name, datetime.fromtimestamp(int(float(value) * 0.001), timezone.utc)
            return self.name, value
        if issubclass(self.type, enum.Enum):
            return self.name, self.type(value)
        return self.name, value


ME = TypeVar("ME", bound="Model")


class Model(abc.ABC):
    def __init__(self, **kwargs):
        pass

    @classmethod
    def _custom_mapping(cls, props: Props, data: Dict[str, Any]):
        pass

    @classmethod
    def from_props(cls: Type[ME], props: Props) -> ME:
        field_map = dict(
            (f.metadata["key"], _PyField(name=f.name, type=f.type, required=f.default is MISSING),)
            for f in fields(cls)
            if "key" in f.metadata
        )
        data = dict(
            (field_map[key].encode(val) for (key, val) in props.items() if key in field_map)
        )
        cls._custom_mapping(props, data)
        self = cls(**data)
        return self

    @classmethod
    def keys(cls):
        class _Keys(dict):
            def __init__(self, iter):
                self.__dict__ = dict(iter)

            def names(self):
                return self.__dict__.keys()

        return _Keys((f.name, f.metadata["key"]) for f in fields(cls))


__all__ = ("Model", "as_list", "Props")
