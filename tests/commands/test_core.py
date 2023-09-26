from dataclasses import dataclass

import pytest
from parametrization import Parametrization

from r3dis.commands.core import Command
from r3dis.commands.parsers import SmartCommandParser, redis_positional_parameter
from r3dis.commands.sorted_sets import RangeMode, SortedSetAdd, SortedSetRange
from r3dis.errors import RedisSyntaxError, RedisWrongNumberOfArguments


@dataclass(eq=True)
class BytesCommand(Command):
    a: bytes = redis_positional_parameter()
    b: bytes = redis_positional_parameter()


@dataclass(eq=True)
class ByteIntCommand(Command):
    a: bytes = redis_positional_parameter()
    c: bool = redis_positional_parameter()
    b: int = redis_positional_parameter()


@dataclass(eq=True)
class ListCommand(Command):
    a: bytes = redis_positional_parameter()
    d: list[int] = redis_positional_parameter()


@Parametrization.autodetect_parameters()
@Parametrization.default_parameters(command_creator=None)
@Parametrization.case(
    name="",
    parameters=[b"a", b"b"],
    expected_command=BytesCommand(b"a", b"b"),
)
@Parametrization.case(
    name="",
    parameters=[b"a", b"1", b"2"],
    expected_command=ByteIntCommand(b"a", True, 2),
)
@Parametrization.case(
    name="",
    parameters=[b"a", b"1", b"2"],
    expected_command=ListCommand(b"a", [1, 2]),
)
@Parametrization.case(
    name="",
    parameters=b"zset (1 5 BYSCORE".split(),
    expected_command=SortedSetRange(None, b"zset", b"(1", b"5", range_mode=RangeMode.BY_SCORE),
    command_creator=lambda *args, **kwargs: SortedSetRange(None, *args, **kwargs),
)
@Parametrization.case(
    name="",
    parameters=b"zset (1 5 BYSCORE rev".split(),
    expected_command=SortedSetRange(None, b"zset", b"(1", b"5", range_mode=RangeMode.BY_SCORE, rev=True),
    command_creator=lambda *args, **kwargs: SortedSetRange(None, *args, **kwargs),
)
@Parametrization.case(
    name="",
    parameters=b'myzset 2 "two" 3 "three',
    expected_command=SortedSetAdd(
        None,
        key=b"myzset",
    ),
    command_creator=lambda *args, **kwargs: SortedSetAdd(None, *args, **kwargs),
)
def test_parser__successful(parameters, expected_command, command_creator):
    if not command_creator:
        command_creator = type(expected_command)
    parser = SmartCommandParser(type(expected_command), command_creator)
    cmd = parser.parse(parameters)
    assert cmd == expected_command


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="",
    command_cls=ByteIntCommand,
    parameters=[b"a", b"a", b"2"],
    expected_exception=RedisSyntaxError,
)
@Parametrization.case(
    name="",
    command_cls=ByteIntCommand,
    parameters=[b"a", b"1"],
    expected_exception=RedisWrongNumberOfArguments,
)
def test_parser__failure(command_cls, parameters, expected_exception):
    parser = SmartCommandParser.from_command_cls(command_cls)
    with pytest.raises(expected_exception):
        parser.parse(parameters)
