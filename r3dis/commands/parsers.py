from dataclasses import MISSING, Field, dataclass, field, fields, is_dataclass
from enum import Enum, StrEnum, auto
from types import UnionType
from typing import Any, T, Union, dataclass_transform, get_args, get_origin

from r3dis.errors import RedisException, RedisSyntaxError, RedisWrongNumberOfArguments


class ParameterMetadata(StrEnum):
    REDIS_PARAMETER = auto()
    VALUES_MAPPING = auto()
    FLAG = auto()


def redis_parameter(
    values_mapping: dict[bool:bytes] = None,
    default=MISSING,
    flag: bytes = None,
):
    metadata = {ParameterMetadata.REDIS_PARAMETER: True}
    if values_mapping:
        metadata[ParameterMetadata.VALUES_MAPPING] = {
            values_mapping[True].upper(): True,
            values_mapping[False].upper(): False,
        }
    if flag:
        if isinstance(flag, dict):
            metadata[ParameterMetadata.FLAG] = {key.upper(): value for key, value in flag.items()}
            default = None
        else:
            metadata[ParameterMetadata.FLAG] = flag.upper()
            metadata[ParameterMetadata.VALUES_MAPPING] = {
                flag.upper(): True,
            }
            default = False
    return field(metadata=metadata, default=default, kw_only=bool(flag))


def redis_positional_parameter(
    bool_map: dict[bool:bytes] = None,
    default=MISSING,
):
    return redis_parameter(
        values_mapping=bool_map,
        default=default,
    )


def redis_keyword_parameter(
    bool_map: dict[bool:bytes] = None,
    default=MISSING,
    flag: bytes | dict[bytes, Any] = None,
):
    return redis_parameter(values_mapping=bool_map, default=default, flag=flag)


@dataclass_transform()
def redis_command(cls: type[T] = None) -> type[T]:
    def wrap(_cls):
        original_order = []

        for name in list(_cls.__dict__.keys()):
            value = getattr(_cls, name)

            if not isinstance(value, Field):
                continue

            if not value.metadata.get(ParameterMetadata.REDIS_PARAMETER):
                continue

            original_order.append(name)

            if not value.kw_only:
                continue

            delattr(_cls, name)
            setattr(_cls, name, value)

        _cls = dataclass(_cls)

        setattr(_cls, "__original_order__", original_order)
        setattr(_cls, "parse", CommandParserCreator.create(_cls))

        return _cls

    if cls is None:
        return wrap

    return wrap(cls)


class ParameterParser:
    @classmethod
    def next_parameter(cls, parameters: list[bytes]) -> bytes:
        try:
            return parameters.pop(0)
        except IndexError:
            raise RedisWrongNumberOfArguments()

    def parse(self, parameters: list[bytes]) -> Any:
        return self.next_parameter(parameters)


class ParametersGroup(ParameterParser):
    def parse(self, parameters: list[bytes]) -> dict[str, Any]:
        raise NotImplementedError()


@dataclass
class NamedParameterParser(ParameterParser):
    name: str
    parameter_parser: ParameterParser

    def parse(self, parameters: list[bytes]) -> Any:
        return self.parameter_parser.parse(parameters)


@dataclass
class ListParameterParser(ParameterParser):
    parameter_parser: ParameterParser

    def parse(self, parameters: list[bytes]) -> list:
        list_parameter = []
        while parameters:
            list_parameter.append(self.parameter_parser.parse(parameters))
        return list_parameter


@dataclass
class SetParameterParser(ParameterParser):
    parameter_parser: ParameterParser

    def parse(self, parameters: list[bytes]) -> set:
        set_value = set()
        while parameters:
            set_value.add(self.parameter_parser.parse(parameters))
        return set_value


@dataclass
class TupleParameterParser(ParameterParser):
    parameter_parser_tuple: tuple[ParameterParser, ...]

    def parse(self, parameters: list[bytes]) -> tuple:
        tuple_parameter = ()
        for parameter_parser in self.parameter_parser_tuple:
            tuple_parameter += (parameter_parser.parse(parameters),)
        return tuple_parameter


class IntParameterParser(ParameterParser):
    def parse(self, parameters: list[bytes]) -> int:
        try:
            return int(self.next_parameter(parameters))
        except ValueError:
            raise RedisException(b"ERR value is not an integer or out of range")


class FloatParameterParser(ParameterParser):
    def parse(self, parameters: list[bytes]) -> float:
        try:
            return float(self.next_parameter(parameters))
        except ValueError:
            raise RedisException(b"ERR value is not a valid float")


@dataclass
class EnumParameterParser(ParameterParser):
    enum_cls: type[Enum]

    def parse(self, parameters: list[bytes]) -> Enum:
        enum_value = self.next_parameter(parameters).upper()
        try:
            return self.enum_cls(enum_value)
        except ValueError as e:
            raise RedisSyntaxError(enum_value) from e


@dataclass
class BoolParameterParser(ParameterParser):
    DEFAULT_VALUES_MAPPING = {b"1": True, b"0": False}

    values_mapping: dict[bytes:bool] = field(default_factory=lambda: BoolParameterParser.DEFAULT_VALUES_MAPPING)

    def parse(self, parameters: list[bytes]) -> bool:
        bytes_value = self.next_parameter(parameters).upper()
        if bytes_value not in self.values_mapping:
            raise RedisSyntaxError(bytes_value)
        return self.values_mapping[bytes_value]


@dataclass
class KeywordParameter(ParameterParser):
    def parse(self, parameters: list[bytes]) -> dict[str, Any]:
        raise NotImplementedError()


@dataclass
class OptionalParametersGroup(ParametersGroup):
    parameters_parsers_map: dict[bytes, NamedParameterParser]

    def parse(self, parameters: list[bytes]) -> dict[str, Any]:
        parsed_kw_parameters = {}

        while parameters:
            top_parameter = parameters[0].upper()
            if top_parameter not in self.parameters_parsers_map:
                return parsed_kw_parameters
            parameter = self.parameters_parsers_map[top_parameter]

            if parameter.name in parsed_kw_parameters:
                raise RedisSyntaxError()
            parsed_kw_parameters[parameter.name] = parameter.parse(parameters)

        return parsed_kw_parameters


@dataclass
class ObjectParametersParser(ParametersGroup):
    parameters_parsers: list[ParameterParser]

    def parse(self, parameters: list[bytes]) -> Any:
        parsed_parameters: dict[str, Any] = {}

        for parameter_parser in self.parameters_parsers:
            if isinstance(parameter_parser, NamedParameterParser):
                parsed_parameters[parameter_parser.name] = parameter_parser.parse(parameters)
            else:
                parsed_parameters.update(parameter_parser.parse(parameters))

        return parsed_parameters

    def __call__(self, parameters: list[bytes]):
        return self.parse(list(parameters))


@dataclass
class ObjectParser(ParametersGroup):
    object_cls: Any
    object_parameters_parser: ObjectParametersParser

    def parse(self, parameters: list[bytes]) -> Any:
        return self.object_cls(self.object_parameters_parser.parse(parameters))


class CommandParserCreator:
    @classmethod
    def extract_optional_type(cls, parameter_type):
        if get_origin(parameter_type) == Union or get_origin(parameter_type) == UnionType:
            items = get_args(parameter_type)
            items = set([arg for arg in items if arg is not type(None)])
            if len(items) > 1:
                raise TypeError(items)
            parameter_type = items.pop()
        return parameter_type

    @classmethod
    def create_parameter_from_field(cls, parameter_field: Field):
        parameter_type = cls.extract_optional_type(parameter_field.type)

        if isinstance(parameter_type, type) and issubclass(parameter_type, Enum):
            return EnumParameterParser(parameter_type)

        if is_dataclass(parameter_type):
            return cls.create_object_parser(parameter_type)

        match parameter_type():
            case bytes():
                return ParameterParser()
            case bool():
                return BoolParameterParser(
                    parameter_field.metadata.get(
                        ParameterMetadata.VALUES_MAPPING, BoolParameterParser.DEFAULT_VALUES_MAPPING
                    )
                )
            case int():
                return IntParameterParser()
            case float():
                return FloatParameterParser()
            case list():
                return cls.create_list_parameter(parameter_field.type.__args__[0])
            case set():
                return cls.create_set_parameter(parameter_field.type.__args__[0])
            case tuple():
                return cls.create_tuple_parameter(parameter_field.type.__args__)
            case default:
                raise TypeError(default)

    @classmethod
    def create_set_parameter(cls, set_type) -> "SetParameterParser":
        match set_type():
            case bytes():
                return SetParameterParser(ParameterParser())
            case int():
                return SetParameterParser(IntParameterParser())
            case default:
                raise TypeError(default)

    @classmethod
    def create_list_parameter(cls, list_type) -> "ListParameterParser":
        match list_type():
            case bytes():
                return ListParameterParser(ParameterParser())
            case int():
                return ListParameterParser(IntParameterParser())
            case tuple():
                return ListParameterParser(cls.create_tuple_parameter(get_args(list_type)))
            case default:
                raise TypeError(default)

    @classmethod
    def create_tuple_parameter(cls, tuple_types) -> "TupleParameterParser":
        parameter_parser_tuple = ()
        for arg in tuple_types:
            match arg():
                case bytes():
                    parameter_parser_tuple += (ParameterParser(),)
                case int():
                    parameter_parser_tuple += (IntParameterParser(),)
                case _:
                    raise TypeError()
        return TupleParameterParser(parameter_parser_tuple)

    @classmethod
    def create_object_parser(cls, object_cls) -> "ObjectParser":
        return ObjectParser(object_cls, cls.create(object_cls))

    @classmethod
    def create(cls, object_cls) -> ObjectParametersParser:
        parameter_fields = {
            parameter_field.name: parameter_field
            for parameter_field in fields(object_cls)
            if parameter_field.metadata.get(ParameterMetadata.REDIS_PARAMETER)
        }
        parameter_fields_by_order = list(parameter_fields.keys())
        if hasattr(object_cls, "__original_order__"):
            parameter_fields_by_order = list(getattr(object_cls, "__original_order__"))

        parameters_parsers: list[ParameterParser] = []

        flagged_fields = {}
        for parameter_field_name in parameter_fields_by_order:
            parameter_field = parameter_fields[parameter_field_name]
            flag = parameter_field.metadata.get(ParameterMetadata.FLAG)
            if flag:
                named_parameter_parser = NamedParameterParser(
                    parameter_field_name, cls.create_parameter_from_field(parameter_field)
                )
                if isinstance(flag, dict):
                    for flag_key in flag.keys():
                        flagged_fields[flag_key] = named_parameter_parser
                else:
                    flagged_fields[flag] = named_parameter_parser
            else:
                if flagged_fields:
                    parameters_parsers.append(OptionalParametersGroup(flagged_fields))
                    flagged_fields = {}

                parameters_parsers.append(
                    NamedParameterParser(parameter_field.name, cls.create_parameter_from_field(parameter_field))
                )

        if flagged_fields:
            parameters_parsers.append(OptionalParametersGroup(flagged_fields))

        return ObjectParametersParser(parameters_parsers)
