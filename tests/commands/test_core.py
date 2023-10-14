from typing import Any

import pytest
from parametrization import Parametrization

from r3dis.commands.core import Command
from r3dis.commands.database_context.sorted_sets import (
    AddMode,
    RangeMode,
    SortedSetAdd,
    SortedSetRange,
)
from r3dis.commands.parsers import redis_command, redis_positional_parameter
from r3dis.errors import RedisSyntaxError, RedisWrongNumberOfArguments


@redis_command()
class BytesCommand(Command):
    a: bytes = redis_positional_parameter()
    b: bytes = redis_positional_parameter()


@redis_command()
class ByteIntCommand(Command):
    a: bytes = redis_positional_parameter()
    c: bool = redis_positional_parameter()
    b: int = redis_positional_parameter()


@redis_command()
class ListCommand(Command):
    a: bytes = redis_positional_parameter()
    d: list[int] = redis_positional_parameter()


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="",
    parameters=[b"a", b"b"],
    command_cls=BytesCommand,
    expected_kwargs={"a": b"a", "b": b"b"},
)
@Parametrization.case(
    name="",
    parameters=[b"a", b"1", b"2"],
    command_cls=ByteIntCommand,
    expected_kwargs={"a": b"a", "b": 2, "c": True},
)
@Parametrization.case(
    name="",
    parameters=[b"a", b"1", b"2"],
    command_cls=ListCommand,
    expected_kwargs={"a": b"a", "d": [1, 2]},
)
@Parametrization.case(
    name="",
    parameters=b"zset (1 5 BYSCORE".split(),
    command_cls=SortedSetRange,
    expected_kwargs={"key": b"zset", "start": b"(1", "stop": b"5", "range_mode": RangeMode.BY_SCORE},
)
@Parametrization.case(
    name="",
    parameters=b"zset (1 5 BYSCORE rev".split(),
    command_cls=SortedSetRange,
    expected_kwargs={"key": b"zset", "start": b"(1", "stop": b"5", "range_mode": RangeMode.BY_SCORE, "rev": True},
)
@Parametrization.case(
    name="",
    parameters=b"myzset 2 two 3 three".split(),
    command_cls=SortedSetAdd,
    expected_kwargs={"key": b"myzset", "scores_members": [(2, b"two"), (3, b"three")]},
)
@Parametrization.case(
    name="",
    parameters=b"myzset NX 2 two 3 three".split(),
    command_cls=SortedSetAdd,
    expected_kwargs={"key": b"myzset", "scores_members": [(2, b"two"), (3, b"three")], "add_mode": AddMode.INSERT_ONLY},
)
def test_parser__successful(parameters, command_cls: Command, expected_kwargs: dict[str, Any]):
    actual_kwargs = command_cls.parse(parameters)
    assert actual_kwargs == expected_kwargs


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="",
    parameters=[b"a", b"a", b"2"],
    expected_exception=RedisSyntaxError,
    command_cls=ByteIntCommand,
)
@Parametrization.case(
    name="",
    parameters=[b"a", b"1"],
    expected_exception=RedisWrongNumberOfArguments,
    command_cls=ByteIntCommand,
)
@Parametrization.case(
    name="",
    parameters=b"myzset NX XX 2 two 3 three".split(),
    expected_exception=RedisSyntaxError,
    command_cls=SortedSetAdd,
)
def test_parser__failure(parameters, expected_exception, command_cls: Command):
    with pytest.raises(expected_exception):
        command_cls.parse(parameters)
