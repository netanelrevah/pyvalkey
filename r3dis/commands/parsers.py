from dataclasses import MISSING, Field, dataclass, field, fields, is_dataclass
from enum import Enum, StrEnum, auto
from types import UnionType
from typing import Any, Callable, Union, get_args, get_origin

from r3dis.commands.core import Command, CommandParser
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
    return field(metadata=metadata, default=default, kw_only=True)


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


@dataclass
class SmartCommandParser(CommandParser):
    command_cls: type[Command]
    command_creator: Callable[[bytes, ...], Command]

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
    def parse_parameter_field(
        cls, parameter_field: Field, parameters: list[bytes]
    ) -> bytes | bool | int | float | list | tuple | set | Enum:
        try:
            parameter_type = cls.extract_optional_type(parameter_field.type)

            if isinstance(parameter_type, type) and issubclass(parameter_type, Enum):
                enum_value = parameters.pop(0).upper()
                try:
                    return parameter_type(enum_value)
                except ValueError as e:
                    raise RedisSyntaxError(enum_value) from e

            if is_dataclass(parameter_type):
                return cls.parse_parameters_object(parameters, parameter_type)

            match parameter_type():
                case bytes():
                    return cls.parse_bytes(parameters)
                case bool():
                    return cls.parse_bool(parameters, parameter_field.metadata.get(ParameterMetadata.VALUES_MAPPING))
                case int():
                    return cls.parse_int(parameters)
                case float():
                    return cls.parse_float(parameters)
                case list():
                    return cls.parse_list(parameters, parameter_field.type.__args__[0])
                case set():
                    return cls.parse_set(parameters, parameter_field.type.__args__[0])
                case tuple():
                    return cls.parse_tuple(parameters, parameter_field.type.__args__)
                case default:
                    raise TypeError(default)
        except IndexError:
            if parameter_field.default == MISSING:
                raise RedisWrongNumberOfArguments()
            return parameter_field.default

    @classmethod
    def parse_parameters_object(cls, parameters: list[bytes], object_cls: Any) -> Any:
        parsed_parameters = []
        for parameter_field in fields(object_cls):
            if not parameter_field.metadata.get(ParameterMetadata.REDIS_PARAMETER):
                continue
            parsed_parameters.append(cls.parse_parameter_field(parameter_field, parameters))

        return object_cls(*parsed_parameters)

    @classmethod
    def parse_bytes(cls, parameters: list[bytes]) -> bytes:
        return parameters.pop(0)

    @classmethod
    def parse_bool(cls, parameters: list[bytes], bool_map: dict[bytes, bool]) -> bool:
        bool_map = bool_map or {b"1": True, b"0": False}

        bytes_value = parameters.pop(0).upper()
        if bytes_value not in bool_map:
            raise RedisSyntaxError(bytes_value)
        return bool_map[bytes_value]

    @classmethod
    def parse_int(cls, parameters: list[bytes]) -> int:
        try:
            return int(parameters.pop(0))
        except ValueError:
            raise RedisException(b"ERR value is not an integer or out of range")

    @classmethod
    def parse_float(cls, parameters: list[bytes]) -> float:
        try:
            return float(parameters.pop(0))
        except ValueError:
            raise RedisException(b"ERR value is not a valid float")

    @classmethod
    def parse_tuple(cls, parameters: list[bytes], tuple_types) -> tuple:
        tuple_parameter = ()
        for arg in tuple_types:
            match arg():
                case bytes():
                    tuple_parameter += (parameters.pop(0),)
                case int():
                    tuple_parameter += (cls.parse_int(parameters),)
                case _:
                    raise TypeError()
        return tuple_parameter

    @classmethod
    def parse_list(cls, parameters: list[bytes], list_type) -> list:
        list_parameter = []
        match list_type():
            case bytes():
                list_parameter.extend(parameters)
                parameters.clear()
            case int():
                while parameters:
                    list_parameter.append(cls.parse_int(parameters))
                return list_parameter
            case tuple():
                while parameters:
                    list_parameter.append(cls.parse_tuple(parameters, get_args(list_type)))
                return list_parameter
            case _:
                raise TypeError()
        return list_parameter

    @classmethod
    def parse_set(cls, parameters: list[bytes], set_type) -> set:
        set_parameter = set()
        match set_type():
            case bytes():
                set_parameter.add(parameters)
                parameters.clear()
            case int():
                while parameters:
                    set_parameter.add(cls.parse_int(parameters))
                return set_parameter
            case _:
                raise TypeError()
        return set_parameter

    def parse_flagged_group(self, flagged_fields, parsed_kw_parameters, parameters: list[bytes]):
        while parameters:
            flag = parameters[0].upper()
            if flag not in flagged_fields:
                raise RedisSyntaxError()
            parameter_field = flagged_fields[flag]

            if parameter_field.name in parsed_kw_parameters:
                raise RedisSyntaxError()
            parsed_kw_parameters[parameter_field.name] = self.parse_parameter_field(parameter_field, parameters)

    def parse(self, parameters: list[bytes]) -> Command:
        parameter_fields = (
            parameter_field
            for parameter_field in fields(self.command_cls)
            if parameter_field.metadata.get(ParameterMetadata.REDIS_PARAMETER)
        )

        parsed_kw_parameters = {}

        flagged_fields = {}
        for parameter_field in parameter_fields:
            flag = parameter_field.metadata.get(ParameterMetadata.FLAG)
            if flag:
                if isinstance(flag, dict):
                    for flag_key, value in flag.items():
                        flagged_fields[flag_key] = parameter_field
                else:
                    flagged_fields[flag] = parameter_field
            else:
                if flagged_fields:
                    self.parse_flagged_group(flagged_fields, parsed_kw_parameters, parameters)
                    flagged_fields = {}
                parsed_kw_parameters[parameter_field.name] = self.parse_parameter_field(parameter_field, parameters)

        return self.command_creator(**parsed_kw_parameters)

    @classmethod
    def from_command_cls(cls, command_cls: type[Command]):
        return cls(command_cls, command_cls)
