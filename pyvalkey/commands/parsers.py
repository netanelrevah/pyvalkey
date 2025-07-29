from __future__ import annotations

import math
from dataclasses import MISSING, Field, dataclass, field, fields, is_dataclass
from enum import Enum, IntEnum
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Self,
    TypeVar,
    Union,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
)

from pyvalkey.commands.consts import LONG_LONG_MAX, LONG_LONG_MIN
from pyvalkey.commands.creators import CommandCreator
from pyvalkey.commands.parameters import ParameterMetadata
from pyvalkey.database_objects.errors import (
    ServerError,
    ServerWrongNumberOfArgumentsError,
)

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command

    CommandType = TypeVar("CommandType", bound=Command)


class ParametersParserCreator:
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
    def create(cls, parameter_field: Field, parameter_type: Any) -> ValueParser:  # noqa: ANN401
        parameter_type = cls._extract_optional_type(parameter_type)

        parse_error = parameter_field.metadata.get(ParameterMetadata.PARSE_ERROR)

        if isinstance(parameter_type, type) and issubclass(parameter_type, Enum):
            return EnumValueParser(parameter_type, parse_error=parse_error)

        if is_dataclass(parameter_type):
            return ObjectValueParser.create(parameter_type)

        match parameter_type():
            case bytes():
                return BytesValueParser()
            case bool():
                return BoolValueParser(
                    parameter_field.metadata.get(
                        ParameterMetadata.VALUES_MAPPING, BoolValueParser.DEFAULT_VALUES_MAPPING
                    )
                )
            case int():
                return IntValueParser(parse_error=parse_error)
            case float():
                return FloatValueParser(parse_error=parse_error)
            case list():
                if parameter_field.metadata.get(ParameterMetadata.MULTI_TOKEN, False):
                    return ParametersParserCreator.create(parameter_field, get_args(parameter_type)[0])
                length_field_name = parameter_field.metadata.get(ParameterMetadata.LENGTH_FIELD_NAME)
                return ListValueParser.create(get_args(parameter_type)[0], length_field_name, parse_error=parse_error)
            case set():
                return SetValueParser.create(get_args(parameter_type)[0], parse_error=parse_error)
            case tuple():
                return TupleValueParser.create(get_args(parameter_type), parse_error=parse_error)
            case default:
                raise TypeError(default)


class ValueParser:
    @classmethod
    def next_parameter(cls, parameters: list[bytes]) -> bytes:
        try:
            return parameters.pop(0)
        except IndexError:
            raise ServerWrongNumberOfArgumentsError()

    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        raise NotImplementedError()

    @classmethod
    def create_parser_from_type(
        cls,
        parameter_type: Any,  # noqa: ANN401
        allow_tuple: bool = False,
        parse_error: bytes | None = None,
    ) -> ValueParser:
        match parameter_type():
            case bytes():
                return BytesValueParser()
            case int():
                return IntValueParser()
            case float():
                return FloatValueParser(parse_error=parse_error)
            case tuple() if allow_tuple:
                return TupleValueParser.create(get_args(parameter_type))
            case default:
                raise TypeError(default)


class BytesValueParser(ValueParser):
    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.next_parameter(parameters)


@dataclass
class ListValueParser(ValueParser):
    parameter_parser: ValueParser
    length_field_name: str | None = None

    def parse(self, parameters: list[bytes]) -> list:
        list_parameter = []
        while parameters:
            if isinstance(self.parameter_parser, TupleValueParser) and len(parameters) < len(
                self.parameter_parser.parameter_parser_tuple
            ):
                break
            list_parameter.append(self.parameter_parser.parse(parameters))
        return list_parameter

    @classmethod
    def create(cls, list_type: Any, length_field_name: str | None = None, parse_error: bytes | None = None) -> Self:  # noqa: ANN401
        return cls(cls.create_parser_from_type(list_type, allow_tuple=True, parse_error=parse_error), length_field_name)


@dataclass
class SetValueParser(ValueParser):
    parameter_parser: ValueParser

    def parse(self, parameters: list[bytes]) -> set:
        set_value = set()
        while parameters:
            set_value.add(self.parameter_parser.parse(parameters))
        return set_value

    @classmethod
    def create(cls, set_type: Any, parse_error: bytes | None = None) -> Self:  # noqa: ANN401
        return cls(cls.create_parser_from_type(set_type, parse_error=parse_error))


@dataclass
class TupleValueParser(ValueParser):
    parameter_parser_tuple: tuple[ValueParser, ...]

    def parse(self, parameters: list[bytes]) -> tuple:
        tuple_parameter = []
        for parameter_parser in self.parameter_parser_tuple:
            tuple_parameter.append(parameter_parser.parse(parameters))
        return tuple(tuple_parameter)

    @classmethod
    def create(cls, tuple_types: tuple[Any, ...], parse_error: bytes | None = None) -> Self:
        return cls(tuple(cls.create_parser_from_type(arg, parse_error=parse_error) for arg in tuple_types))


@dataclass
class IntValueParser(ValueParser):
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


@dataclass
class FloatValueParser(ValueParser):
    parse_error: bytes | None = None

    def parse(self, parameters: list[bytes]) -> float:
        try:
            value = float(self.next_parameter(parameters))
            if math.isnan(value):
                raise ValueError()
        except ValueError:
            if self.parse_error is not None:
                raise ServerError(self.parse_error)
            raise ServerError(b"ERR value is not a valid float")
        return value


@dataclass
class EnumValueParser(ValueParser):
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
                raise ServerError(self.parse_error) from e
            raise ServerError(b"ERR syntax error") from e


@dataclass
class BoolValueParser(ValueParser):
    DEFAULT_VALUES_MAPPING: ClassVar = {b"1": True, b"0": False}

    values_mapping: dict[bytes, bool] = field(default_factory=lambda: BoolValueParser.DEFAULT_VALUES_MAPPING)

    def parse(self, parameters: list[bytes]) -> bool:
        bytes_value = self.next_parameter(parameters).upper()
        if bytes_value not in self.values_mapping:
            raise ServerError(b"ERR syntax error")
        return self.values_mapping[bytes_value]


@dataclass
class ObjectValueParser(ValueParser):
    object_cls: Any
    object_parameters_parser: ObjectParametersParser

    def parse(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.object_cls(**self.object_parameters_parser.parse(parameters))

    @classmethod
    def create(cls, object_cls: Any) -> Self:  # noqa: ANN401
        return cls(object_cls, ObjectParametersParser.create(object_cls, allow_more_parameters=True))


@dataclass
class ParameterParserContext:
    parsed_parameters: dict[str, Any]
    parameters_parsers: list[ParameterParser]

    left_parameter_parsers: list[ParameterParser]


class ParameterParser:
    def parse(self, parameters: list[bytes], context: ParameterParserContext | None = None) -> Any:  # noqa: ANN401
        raise NotImplementedError()

    @property
    def is_optional(self) -> bool:
        raise NotImplementedError()


@dataclass
class NamedParameterParser(ParameterParser):
    name: str
    parameter_parser: ValueParser

    is_optional: bool = False

    def parse(self, parameters: list[bytes], context: ParameterParserContext | None = None) -> dict[str, Any]:
        return {self.name: self.parameter_parser.parse(parameters)}

    @classmethod
    def create(
        cls,
        name: str,
        parameter_parser: ValueParser,
        is_optional: bool = False,
        length_field_name: str | None = None,
        errors: dict[str, bytes] | None = None,
        allow_empty: bool = True,
    ) -> NamedParameterParser:
        if isinstance(parameter_parser, ListValueParser | SetValueParser):
            return SequenceNamedParameterParser(
                name,
                parameter_parser,
                length_field_name=length_field_name,
                is_optional=is_optional,
                **(errors or {}),
                allow_empty=allow_empty,
            )
        return NamedParameterParser(name, parameter_parser, is_optional=is_optional)


@dataclass
class SequenceNamedParameterParser(NamedParameterParser):
    length_field_name: str | None = None
    parse_error: bytes | None = None

    when_length_field_less_then_parameters: bytes | None = None
    when_length_field_more_then_parameters: bytes | None = None
    allow_empty: bool = True

    def parse(self, parameters: list[bytes], context: ParameterParserContext | None = None) -> dict[str, Any]:
        if context is None:
            raise ValueError("Context must be provided for SequenceNamedParameterParser")
        if self.length_field_name is not None:
            length = context.parsed_parameters.get(self.length_field_name, 0)
            if length <= 0:
                if self.when_length_field_less_then_parameters is not None:
                    raise ServerError(self.when_length_field_less_then_parameters)
                raise ServerError(f"ERR {self.length_field_name} should be greater than 0".encode())
            if len(parameters) == 0:
                raise ServerWrongNumberOfArgumentsError()
            if len(parameters) < length:
                if self.when_length_field_more_then_parameters is not None:
                    raise ServerError(self.when_length_field_more_then_parameters)
                raise ServerWrongNumberOfArgumentsError()
            parameters = [parameters.pop(0) for _ in range(length)]
        elif len(context.left_parameter_parsers) > 0:
            parameters = [parameters.pop(0) for _ in range(len(parameters) - len(context.left_parameter_parsers))]
        elif not self.allow_empty and len(parameters) == 0:
            raise ServerWrongNumberOfArgumentsError()
        return {self.name: self.parameter_parser.parse(parameters)}


@dataclass
class OptionalKeywordParameter:
    parameter: NamedParameterParser
    has_token: bool = False
    is_multi: bool = False
    skip_first: bool = False


@dataclass
class OptionalKeywordParametersGroup(ParameterParser):
    parameters_parsers_map: dict[bytes, OptionalKeywordParameter]

    @property
    def is_optional(self) -> bool:
        return True

    def parse(self, parameters: list[bytes], context: ParameterParserContext | None = None) -> dict[str, Any]:
        parsed_kw_parameters: dict[str, Any] = {}

        while parameters:
            top_parameter = parameters[0].upper()
            if top_parameter not in self.parameters_parsers_map:
                return parsed_kw_parameters
            keyword_parameter = self.parameters_parsers_map[top_parameter]

            if keyword_parameter.has_token:
                parameters.pop(0)
                if not parameters:
                    raise ServerError(b"ERR syntax error")

            if keyword_parameter.is_multi:
                parsed = keyword_parameter.parameter.parse(parameters, context)
                if keyword_parameter.parameter.name not in parsed_kw_parameters:
                    parsed_kw_parameters[keyword_parameter.parameter.name] = [parsed[keyword_parameter.parameter.name]]
                else:
                    parsed_kw_parameters[keyword_parameter.parameter.name].append(
                        parsed[keyword_parameter.parameter.name]
                    )
            else:
                if not keyword_parameter.skip_first and keyword_parameter.parameter.name in parsed_kw_parameters:
                    raise ServerError(b"ERR syntax error")
                parsed_kw_parameters.update(keyword_parameter.parameter.parse(parameters, context))

        return parsed_kw_parameters


@dataclass
class ObjectParametersParser(ParameterParser):
    parameters_parsers: list[ParameterParser]
    allow_more_parameters: bool = False

    def parse(self, parameters: list[bytes], context: ParameterParserContext | None = None) -> Any:  # noqa: ANN401
        parsed_parameters: dict[str, Any] = {}

        non_optional_parameters = sum(1 for p in self.parameters_parsers if not p.is_optional)

        for index, parameter_parser in enumerate(self.parameters_parsers):
            if parameter_parser.is_optional:
                if len(parameters) <= (non_optional_parameters - index):
                    continue

                if index + 1 < len(self.parameters_parsers):
                    next_parameters_parser = self.parameters_parsers[index + 1]
                    if (
                        isinstance(next_parameters_parser, OptionalKeywordParametersGroup)
                        and parameters[0] in next_parameters_parser.parameters_parsers_map
                    ):
                        continue

            parsed_parameters.update(
                parameter_parser.parse(
                    parameters,
                    ParameterParserContext(
                        parsed_parameters=parsed_parameters,
                        parameters_parsers=self.parameters_parsers,
                        left_parameter_parsers=self.parameters_parsers[index + 1 :],
                    ),
                )
            )

        if parameters and not self.allow_more_parameters:
            has_optional_keyword_parameters = False
            for parameter_parser in self.parameters_parsers:
                if isinstance(parameter_parser, OptionalKeywordParametersGroup):
                    has_optional_keyword_parameters = True
                    break
            if has_optional_keyword_parameters:
                raise ServerError(b"ERR syntax error")

            raise ServerWrongNumberOfArgumentsError()

        return parsed_parameters

    def __call__(self, parameters: list[bytes]) -> Any:  # noqa: ANN401
        return self.parse(list(parameters))

    @classmethod
    def create(cls, object_cls: Any, allow_more_parameters: bool = False) -> Self:  # noqa: ANN401
        resolved_hints = get_type_hints(object_cls)

        parameter_fields = {
            parameter_field.name: parameter_field
            for parameter_field in fields(object_cls)
            if parameter_field.metadata.get(ParameterMetadata.COMMAND_PARAMETER)
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
            length_field_name = parameter_field.metadata.get(ParameterMetadata.LENGTH_FIELD_NAME, None)
            if flag:
                named_parameter_parser = NamedParameterParser.create(
                    parameter_field_name,
                    ParametersParserCreator.create(parameter_field, resolved_hints[parameter_field.name]),
                    length_field_name=length_field_name,
                    errors=parameter_field.metadata.get(ParameterMetadata.ERRORS, None),
                    allow_empty=parameter_field.metadata.get(ParameterMetadata.SEQUENCE_ALLOW_EMPTY, True),
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
                        NamedParameterParser.create(
                            parameter_field.name,
                            ParametersParserCreator.create(parameter_field, resolved_hints[parameter_field.name]),
                            length_field_name=length_field_name,
                            is_optional=True,
                            errors=parameter_field.metadata.get(ParameterMetadata.ERRORS, None),
                            allow_empty=parameter_field.metadata.get(ParameterMetadata.SEQUENCE_ALLOW_EMPTY, True),
                        )
                    )
                else:
                    parameters_parsers.append(
                        NamedParameterParser.create(
                            parameter_field.name,
                            ParametersParserCreator.create(parameter_field, resolved_hints[parameter_field.name]),
                            length_field_name=length_field_name,
                            errors=parameter_field.metadata.get(ParameterMetadata.ERRORS, None),
                            allow_empty=parameter_field.metadata.get(ParameterMetadata.SEQUENCE_ALLOW_EMPTY, True),
                        )
                    )

        if optional_keyword_parameters:
            parameters_parsers.append(OptionalKeywordParametersGroup(optional_keyword_parameters))

        return cls(parameters_parsers, allow_more_parameters=allow_more_parameters)


@dataclass_transform()
def move_mandatory_field_to_start(command_cls: type[CommandType]) -> list[str]:
    cls_annotations = getattr(command_cls, "__annotations__", {})

    original_order = []
    for name in list(command_cls.__dict__.keys()):
        value = getattr(command_cls, name)

        if not isinstance(value, Field):
            continue

        if not value.metadata.get(ParameterMetadata.COMMAND_PARAMETER):
            continue

        original_order.append(name)

        if not value.kw_only and value.default == MISSING:
            continue

        if name in cls_annotations:
            annotation = cls_annotations[name]
            del cls_annotations[name]
            cls_annotations[name] = annotation

        delattr(command_cls, name)
        setattr(command_cls, name, value)

    return original_order


@dataclass_transform()
def transform_command(command_cls: type[CommandType]) -> type[CommandType]:
    original_order = move_mandatory_field_to_start(command_cls)

    command_cls = dataclass(command_cls)
    setattr(command_cls, "__original_order__", original_order)
    setattr(command_cls, "parse", ObjectParametersParser.create(command_cls))
    setattr(command_cls, "create", CommandCreator.create(command_cls))

    return command_cls
