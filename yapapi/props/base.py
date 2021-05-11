from typing import Dict, Type, Any, Union, List, cast, TypeVar
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore

import typing
import abc
import enum
import json
from dataclasses import dataclass, fields, MISSING, field, Field
from datetime import datetime, timezone

Props = Dict[str, str]

PROP_KEY = "key"
PROP_OPERATOR = "operator"
PROP_MODEL_FIELD_TYPE = "model_field_type"


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


class InvalidPropertiesError(Exception):
    """Raised by `Model.from_properties(cls, properties)` when given invalid `properties`."""

    def __str__(self):
        msg = "Invalid properties"
        if self.args:
            msg += f": {self.args[0]}"
        return msg


@dataclass
class Model(abc.ABC):
    """
    Base class from which all property models inherit.

    Provides helper methods to load the property model data from a dictionary and
    to get a mapping of all the keys available in the given model.
    """

    def __init__(self, **kwargs):
        pass

    @classmethod
    def _custom_mapping(cls, props: Props, data: Dict[str, Any]):
        pass

    @classmethod
    def property_fields(cls):
        return (
            f
            for f in fields(cls)
            if PROP_KEY in f.metadata
               and f.metadata.get(PROP_MODEL_FIELD_TYPE, ModelFieldType.property) == ModelFieldType.property
        )

    @classmethod
    def from_properties(cls: Type[ME], props: Props) -> ME:
        """
        Initialize the model from a dictionary representation.

        When provided with a dictionary of properties, it will find the matching keys
        within it and fill the model fields with the values from the dictionary.

        It ignores non-matching keys - i.e. doesn't require filtering of the properties'
        dictionary before the model is fed with the data. Thus, several models can be
        initialized from the same dictionary and all models will only load their own data.
        """
        field_map = dict(
            (
                f.metadata[PROP_KEY],
                _PyField(name=f.name, type=f.type, required=f.default is MISSING),
            )
            for f in cls.property_fields()
        )
        data = dict(
            (field_map[key].encode(val) for (key, val) in props.items() if key in field_map)
        )
        try:
            cls._custom_mapping(props, data)
            self = cls(**data)
            return self
        except Exception as exc:
            # Handle some common cases to improve error diagnostics
            if isinstance(exc, KeyError):
                msg = f"Missing key: {exc}"
            elif isinstance(exc, json.JSONDecodeError):
                msg = f"Error when decoding '{exc.doc}': {exc}"
            else:
                msg = f"{exc}"
            raise InvalidPropertiesError(msg) from exc

    @classmethod
    def property_keys(cls):
        """
        :return: a mapping between the model's field names and the property keys

        example:
        ```python
        >>> import dataclasses
        >>> import typing
        >>> from yapapi.props.base import Model
        >>> @dataclasses.dataclass
        ... class NodeInfo(Model):
        ...     name: typing.Optional[str] = \
        ...     dataclasses.field(default=None, metadata={"key": "golem.node.id.name"})
        ...
        >>> NodeInfo.property_keys().name
        'golem.node.id.name'
        ```
        """

        class _Keys:
            def __init__(self, iter):
                self.__dict__ = dict(iter)

            def names(self):
                return self.__dict__.keys()

        return _Keys((f.name, f.metadata["key"]) for f in cls.property_fields())


class ConstraintException(Exception):
    pass


CONSTRAINT_VAL_ANY = "*"

ConstraintOperator = Literal['=', ">=", "<="]
ConstraintGroupOperator = Literal["&", "|", "!"]


class ModelFieldType(enum.Enum):
    constraint = "constraint"
    property = "property"


def constraint(key: str, *, operator: ConstraintOperator = "=", default=MISSING):
    """return a contraint-type dataclass field"""
    return field(
        default=default,
        metadata={
            PROP_KEY: key,
            PROP_OPERATOR: operator,
            PROP_MODEL_FIELD_TYPE: ModelFieldType.constraint,
        }
    )


def prop(key: str, *, default=MISSING):
    """return a property-type dataclass field"""
    return field(
        default=default,
        metadata={
            PROP_KEY: key,
            PROP_MODEL_FIELD_TYPE: ModelFieldType.property
        }
    )


def constraint_to_str(value, f: Field) -> str:
    return f"({f.metadata[PROP_KEY]}{f.metadata[PROP_OPERATOR]}{value})"


def constraint_model_serialize(m: Model) -> List[str]:
    return [
        constraint_to_str(getattr(m, f.name), f)
        for f in fields(type(m))
        if f.metadata.get(PROP_MODEL_FIELD_TYPE, "") == ModelFieldType.constraint
    ]


def join_str_constraints(constraints: List[str], operator: ConstraintGroupOperator = "&"):
    if not constraints:
        return "()"

    if operator == "!":
       if len(constraints) == 1:
           return f"({operator}({constraints[0]}))"
       else:
           raise ConstraintException(f"{operator} requires exactly one component.")

    if len(constraints) == 1:
        return f"({constraints[0]})"

    rules = "\n\t".join(constraints)
    return f"({operator}{rules})"


__all__ = ("Model", "as_list", "Props")
