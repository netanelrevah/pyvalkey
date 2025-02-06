from __future__ import annotations

from collections.abc import Callable
from dataclasses import MISSING, Field, dataclass, field, fields, is_dataclass
from enum import Enum, IntEnum
from types import UnionType
from typing import TYPE_CHECKING, Any, ClassVar, Self, Union, get_args, get_origin, get_type_hints, overload

from pyvalkey.commands.consts import LONG_LONG_MAX, LONG_LONG_MIN
from pyvalkey.commands.creators import CommandCreator
from pyvalkey.commands.parameters import ParameterMetadata
from pyvalkey.database_objects.errors import (
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ValkeySyntaxError,
)

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command


class ParameterParser:
    @classmethod
    def next_parameter(cls, parameters: list[bytes]) -> bytes:
        try:
            return parameters.pop(0)
        except IndexError:
            raise ServerWrongNumberOfArgumentsError()

    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.next_parameter(parameters)

    @classmethod
    def _extract_optional_type(cls, parameter_type: Any) -> Any:  # noqa: ANN401
        if get_origin(parameter_type) == Union or get_origin(parameter_type) == UnionType:
            args = get_args(parameter_type)
            items = set([arg for arg in args if arg is not type(None)])
            if len(items) > 1:
                raise TypeError(items)
            parameter_type = items.pop()
        return parameter_type

    @classmethod
    def create(cls, parameter_field: Field, parameter_type: Any, is_multi: bool = False) -> ParameterParser:  # noqa: ANN401
        parameter_type = cls._extract_optional_type(parameter_type)

        parse_error = parameter_field.metadata.get(ParameterMetadata.PARSE_ERROR)

        if isinstance(parameter_type, type) and issubclass(parameter_type, Enum):
            return EnumParameterParser(parameter_type, parse_error=parse_error)

        if is_dataclass(parameter_type):
            return ObjectParser.create_from_object(parameter_type)

        match parameter_type():
            case bytes():
                if parameter_field.metadata.get(ParameterMetadata.KEY_MODE, False):
                    return KeyParameterParser()
                return ParameterParser()
            case bool():
                return BoolParameterParser(
                    parameter_field.metadata.get(
                        ParameterMetadata.VALUES_MAPPING, BoolParameterParser.DEFAULT_VALUES_MAPPING
                    )
                )
            case int():
                return IntParameterParser(parse_error=parse_error)
            case float():
                return FloatParameterParser()
            case list():
                if is_multi:
                    return ParameterParser.create(parameter_field, get_args(parameter_type)[0])
                return ListParameterParser.create_from_list_type(get_args(parameter_type)[0])
            case set():
                return SetParameterParser.create_from_type(get_args(parameter_type)[0])
            case tuple():
                return TupleParameterParser.create_from_tuple_types(get_args(parameter_type))
            case default:
                raise TypeError(default)


class KeyParameterParser(ParameterParser):
    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.next_parameter(parameters)


class ParametersGroup(ParameterParser):
    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        raise NotImplementedError()


@dataclass
class NamedParameterParser(ParameterParser):
    name: str
    parameter_parser: ParameterParser

    def parse(self, parameters: list[bytes]) -> dict[str, Any]:
        return {self.name: self.parameter_parser.parse(parameters)}


@dataclass
class OptionalNamedParameterParser(NamedParameterParser):
    pass


@dataclass
class ListParameterParser(ParameterParser):
    parameter_parser: ParameterParser

    def parse(self, parameters: list[bytes]) -> list:
        list_parameter = []
        while parameters:
            list_parameter.append(self.parameter_parser.parse(parameters))
        return list_parameter

    @classmethod
    def create_from_list_type(cls, list_type: Any) -> Self:  # noqa: ANN401
        match list_type():
            case bytes():
                return cls(ParameterParser())
            case int():
                return cls(IntParameterParser())
            case tuple():
                return cls(TupleParameterParser.create_from_tuple_types(get_args(list_type)))
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
    def create_from_type(cls, set_type: Any) -> Self:  # noqa: ANN401
        match set_type():
            case bytes():
                return cls(ParameterParser())
            case int():
                return cls(IntParameterParser())
            case default:
                raise TypeError(default)


@dataclass
class TupleParameterParser(ParameterParser):
    parameter_parser_tuple: tuple[ParameterParser, ...]

    def parse(self, parameters: list[bytes]) -> tuple:
        tuple_parameter = []
        for parameter_parser in self.parameter_parser_tuple:
            tuple_parameter.append(parameter_parser.parse(parameters))
        return tuple(tuple_parameter)

    @classmethod
    def create_from_tuple_types(cls, tuple_types: tuple[Any, ...]) -> Self:
        parameter_parser_tuple = []
        for arg in tuple_types:
            match arg():
                case bytes():
                    parameter_parser_tuple.append(ParameterParser())
                case int():
                    parameter_parser_tuple.append(IntParameterParser())
                case _:
                    raise TypeError()
        return cls(tuple(parameter_parser_tuple))


@dataclass
class IntParameterParser(ParameterParser):
    parse_error: bytes | None = None

    def parse(self, parameters: list[bytes]) -> int:
        try:
            value = int(self.next_parameter(parameters))
            if not (LONG_LONG_MIN <= value <= LONG_LONG_MAX):
                raise ServerError(b"ERR value is not an integer or out of range")
            return value
        except ValueError:
            if self.parse_error is not None:
                raise ServerError(self.parse_error)
            raise ServerError(b"ERR value is not an integer or out of range")


class FloatParameterParser(ParameterParser):
    def parse(self, parameters: list[bytes]) -> float:
        try:
            return float(self.next_parameter(parameters))
        except ValueError:
            raise ServerError(b"ERR value is not a valid float")


@dataclass
class EnumParameterParser(ParameterParser):
    enum_cls: type[Enum]

    parse_error: bytes | None = None

    def parse(self, parameters: list[bytes]) -> Enum:
        enum_value = self.next_parameter(parameters).upper()
        try:
            if issubclass(self.enum_cls, IntEnum):
                return self.enum_cls(int(enum_value))
            return self.enum_cls(enum_value)
        except ValueError as e:
            if self.parse_error is not None:
                raise ServerError(self.parse_error)
            raise ValkeySyntaxError(enum_value) from e


@dataclass
class BoolParameterParser(ParameterParser):
    DEFAULT_VALUES_MAPPING: ClassVar = {b"1": True, b"0": False}

    values_mapping: dict[bytes, bool] = field(default_factory=lambda: BoolParameterParser.DEFAULT_VALUES_MAPPING)

    def parse(self, parameters: list[bytes]) -> bool:
        bytes_value = self.next_parameter(parameters).upper()
        if bytes_value not in self.values_mapping:
            raise ValkeySyntaxError(bytes_value)
        return self.values_mapping[bytes_value]


@dataclass
class OptionalKeywordParameter:
    parameter: NamedParameterParser
    has_token: bool = False
    is_multi: bool = False
    skip_first: bool = False


@dataclass
class OptionalKeywordParametersGroup(ParametersGroup):
    parameters_parsers_map: dict[bytes, OptionalKeywordParameter]

    def parse(self, parameters: list[bytes]) -> dict[str, Any]:
        parsed_kw_parameters: dict[str, Any] = {}

        while parameters:
            top_parameter = parameters[0].upper()
            if top_parameter not in self.parameters_parsers_map:
                return parsed_kw_parameters
            keyword_parameter = self.parameters_parsers_map[top_parameter]

            if keyword_parameter.has_token:
                parameters.pop(0)

            if keyword_parameter.is_multi:
                parsed = keyword_parameter.parameter.parse(parameters)
                if keyword_parameter.parameter.name not in parsed_kw_parameters:
                    parsed_kw_parameters[keyword_parameter.parameter.name] = [parsed[keyword_parameter.parameter.name]]
                else:
                    parsed_kw_parameters[keyword_parameter.parameter.name].append(
                        parsed[keyword_parameter.parameter.name]
                    )
            else:
                if not keyword_parameter.skip_first and keyword_parameter.parameter.name in parsed_kw_parameters:
                    raise ValkeySyntaxError()
                parsed_kw_parameters.update(keyword_parameter.parameter.parse(parameters))

        return parsed_kw_parameters


@dataclass
class ObjectParametersParser(ParametersGroup):
    parameters_parsers: list[ParameterParser]

    @classmethod
    def _is_optional(cls, parameter_parser: ParameterParser) -> bool:
        return isinstance(parameter_parser, OptionalKeywordParametersGroup | OptionalNamedParameterParser)

    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        parsed_parameters: dict[str, Any] = {}

        non_optional_parameters = sum(1 for p in self.parameters_parsers if not self._is_optional(p))

        for index, parameter_parser in enumerate(self.parameters_parsers):
            if self._is_optional(parameter_parser):
                if len(parameters) <= (non_optional_parameters - index):
                    continue

                if index + 1 < len(self.parameters_parsers):
                    next_parameters_parser = self.parameters_parsers[index + 1]
                    if (
                        isinstance(next_parameters_parser, OptionalKeywordParametersGroup)
                        and parameters[0] in next_parameters_parser.parameters_parsers_map
                    ):
                        continue

            parsed_parameters.update(parameter_parser.parse(parameters))

        if parameters:
            raise ServerWrongNumberOfArgumentsError()
            # if non_optional_parameters == 0:
            # else:
            #     raise ValkeySyntaxError()

        return parsed_parameters

    def __call__(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.parse(list(parameters))

    @classmethod
    def create_from_object(cls, object_cls: Any) -> Self:  # noqa: ANN401
        resolved_hints = get_type_hints(object_cls)

        parameter_fields = {
            parameter_field.name: parameter_field
            for parameter_field in fields(object_cls)
            if parameter_field.metadata.get(ParameterMetadata.SERVER_PARAMETER)
        }
        parameter_fields_by_order = list(parameter_fields.keys())
        if hasattr(object_cls, "__original_order__"):
            parameter_fields_by_order = list(getattr(object_cls, "__original_order__"))

        parameters_parsers: list[ParameterParser] = []

        optional_keyword_parameters = {}
        for parameter_field_name in parameter_fields_by_order:
            parameter_field = parameter_fields[parameter_field_name]
            flag = parameter_field.metadata.get(ParameterMetadata.TOKEN)
            is_multi = parameter_field.metadata.get(ParameterMetadata.MULTI_TOKEN, False)
            skip_first = parameter_field.metadata.get(ParameterMetadata.SKIP_FIRST, False)
            if flag:
                named_parameter_parser = NamedParameterParser(
                    parameter_field_name,
                    ParameterParser.create(parameter_field, resolved_hints[parameter_field.name], is_multi),
                )
                if isinstance(flag, dict):
                    for flag_key in flag.keys():
                        optional_keyword_parameters[flag_key] = OptionalKeywordParameter(named_parameter_parser, False)
                elif isinstance(parameter_field.default, bool):
                    optional_keyword_parameters[flag] = OptionalKeywordParameter(named_parameter_parser, False)
                else:
                    optional_keyword_parameters[flag] = OptionalKeywordParameter(
                        named_parameter_parser, True, is_multi, skip_first=skip_first
                    )
            else:
                if optional_keyword_parameters:
                    parameters_parsers.append(OptionalKeywordParametersGroup(optional_keyword_parameters))
                    optional_keyword_parameters = {}

                if parameter_field.default != MISSING:
                    parameters_parsers.append(
                        OptionalNamedParameterParser(
                            parameter_field.name,
                            ParameterParser.create(parameter_field, resolved_hints[parameter_field.name]),
                        )
                    )
                else:
                    parameters_parsers.append(
                        NamedParameterParser(
                            parameter_field.name,
                            ParameterParser.create(parameter_field, resolved_hints[parameter_field.name]),
                        )
                    )

        if optional_keyword_parameters:
            parameters_parsers.append(OptionalKeywordParametersGroup(optional_keyword_parameters))

        return cls(parameters_parsers)


@dataclass
class ObjectParser(ParametersGroup):
    object_cls: Any
    object_parameters_parser: ObjectParametersParser

    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.object_cls(**self.object_parameters_parser.parse(parameters))

    @classmethod
    def create_from_object(cls, object_cls: Any) -> Self:  # noqa: ANN401
        return cls(object_cls, ObjectParametersParser.create_from_object(object_cls))


def _server_command_wrapper(command_cls: type[Command]) -> type[Command]:
    original_order = []

    annotations = getattr(command_cls, "__annotations__", {})

    for name in list(command_cls.__dict__.keys()):
        value = getattr(command_cls, name)

        if not isinstance(value, Field):
            continue

        if not value.metadata.get(ParameterMetadata.SERVER_PARAMETER):
            continue

        original_order.append(name)

        if not value.kw_only and value.default == MISSING:
            continue

        if name in annotations:
            annotation = annotations[name]
            del annotations[name]
            annotations[name] = annotation

        delattr(command_cls, name)
        setattr(command_cls, name, value)

    command_cls = dataclass(command_cls)

    setattr(command_cls, "__original_order__", original_order)
    setattr(command_cls, "parse", ObjectParametersParser.create_from_object(command_cls))
    setattr(command_cls, "create", CommandCreator.create(command_cls))

    return command_cls


@overload
def server_command(command_cls: Callable[[type[Command]], type[Command]], /) -> type[Command]: ...


@overload
def server_command(*args: type[Command]) -> type[Command]: ...


def server_command(*args: Any) -> Any:
    if len(args) == 0:
        return _server_command_wrapper
    return _server_command_wrapper(args[0])
