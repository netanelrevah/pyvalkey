from dataclasses import Field, dataclass, field, fields, is_dataclass
from enum import Enum
from types import UnionType
from typing import Any, Union, get_args, get_origin

from typing_extensions import Self

from r3dis.commands.creators import CommandCreator
from r3dis.commands.parameters import ParameterMetadata
from r3dis.errors import RedisException, RedisSyntaxError, RedisWrongNumberOfArguments


class ParameterParser:
    @classmethod
    def next_parameter(cls, parameters: list[bytes]) -> bytes:
        try:
            return parameters.pop(0)
        except IndexError:
            raise RedisWrongNumberOfArguments()

    def parse(self, parameters: list[bytes]) -> Any:
        return self.next_parameter(parameters)

    @classmethod
    def _extract_optional_type(cls, parameter_type):
        if get_origin(parameter_type) == Union or get_origin(parameter_type) == UnionType:
            items = get_args(parameter_type)
            items = set([arg for arg in items if arg is not type(None)])
            if len(items) > 1:
                raise TypeError(items)
            parameter_type = items.pop()
        return parameter_type

    @classmethod
    def create(cls, parameter_field: Field):
        parameter_type = cls._extract_optional_type(parameter_field.type)

        if isinstance(parameter_type, type) and issubclass(parameter_type, Enum):
            return EnumParameterParser(parameter_type)

        if is_dataclass(parameter_type):
            return ObjectParser.create(parameter_type)

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
                return ListParameterParser.create(get_args(parameter_type)[0])
            case set():
                return SetParameterParser.create(get_args(parameter_type)[0])
            case tuple():
                return TupleParameterParser.create(get_args(parameter_type))
            case default:
                raise TypeError(default)


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

    @classmethod
    def create(cls, list_type) -> "ListParameterParser":
        match list_type():
            case bytes():
                return ListParameterParser(ParameterParser())
            case int():
                return ListParameterParser(IntParameterParser())
            case tuple():
                return ListParameterParser(TupleParameterParser.create(get_args(list_type)))
            case default:
                raise TypeError(default)


@dataclass
class SetParameterParser(ParameterParser):
    parameter_parser: ParameterParser

    def parse(self, parameters: list[bytes]) -> set:
        set_value = set()
        while parameters:
            set_value.add(self.parameter_parser.parse(parameters))
        return set_value

    @classmethod
    def create(cls, set_type) -> "SetParameterParser":
        match set_type():
            case bytes():
                return SetParameterParser(ParameterParser())
            case int():
                return SetParameterParser(IntParameterParser())
            case default:
                raise TypeError(default)


@dataclass
class TupleParameterParser(ParameterParser):
    parameter_parser_tuple: tuple[ParameterParser, ...]

    def parse(self, parameters: list[bytes]) -> tuple:
        tuple_parameter = ()
        for parameter_parser in self.parameter_parser_tuple:
            tuple_parameter += (parameter_parser.parse(parameters),)
        return tuple_parameter

    @classmethod
    def create(cls, tuple_types) -> Self:
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

    @classmethod
    def create(cls, object_cls) -> Self:
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
                    parameter_field_name, ParameterParser.create(parameter_field)
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
                    NamedParameterParser(parameter_field.name, ParameterParser.create(parameter_field))
                )

        if flagged_fields:
            parameters_parsers.append(OptionalParametersGroup(flagged_fields))

        return ObjectParametersParser(parameters_parsers)


@dataclass
class ObjectParser(ParametersGroup):
    object_cls: Any
    object_parameters_parser: ObjectParametersParser

    def parse(self, parameters: list[bytes]) -> Any:
        return self.object_cls(self.object_parameters_parser.parse(parameters))

    @classmethod
    def create(cls, object_cls) -> "ObjectParser":
        return ObjectParser(object_cls, ObjectParametersParser.create(object_cls))


def _redis_command_wrapper(command_cls):
    original_order = []

    for name in list(command_cls.__dict__.keys()):
        value = getattr(command_cls, name)

        if not isinstance(value, Field):
            continue

        if not value.metadata.get(ParameterMetadata.REDIS_PARAMETER):
            continue

        original_order.append(name)

        if not value.kw_only:
            continue

        delattr(command_cls, name)
        setattr(command_cls, name, value)

    command_cls = dataclass(command_cls)

    setattr(command_cls, "__original_order__", original_order)
    setattr(command_cls, "parse", ObjectParametersParser.create(command_cls))
    setattr(command_cls, "create", CommandCreator.create(command_cls))

    return command_cls


def redis_command(command_cls=None):
    if command_cls is None:
        return _redis_command_wrapper
    return _redis_command_wrapper(command_cls)
