from typing import Any

import pytest
from parametrization import Parametrization

from pyvalkey.commands.clients import ClientKill
from pyvalkey.commands.core import Command
from pyvalkey.commands.debug import Debug
from pyvalkey.commands.keyspace_commands import Copy
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.parsers import server_command
from pyvalkey.commands.sorted_sets import AddMode, RangeMode, SortedSetAdd, SortedSetRange
from pyvalkey.commands.string_commands import Ping
from pyvalkey.database_objects.errors import ServerWrongNumberOfArgumentsError, ValkeySyntaxError


@server_command()
class BytesCommand(Command):
    a: bytes = positional_parameter()
    b: bytes = positional_parameter()


@server_command()
class ByteIntCommand(Command):
    a: bytes = positional_parameter()
    c: bool = positional_parameter()
    b: int = positional_parameter()


@server_command()
class ListCommand(Command):
    a: bytes = positional_parameter()
    d: list[int] = positional_parameter()


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
    name="zrange_with_kw_range_mode",
    parameters=b"zset (1 5 BYSCORE".split(),
    command_cls=SortedSetRange,
    expected_kwargs={"key": b"zset", "start": b"(1", "stop": b"5", "range_mode": RangeMode.BY_SCORE},
)
@Parametrization.case(
    name="zrange_with_rev_flag",
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
@Parametrization.case(
    name="",
    parameters=b"a b".split(),
    command_cls=Copy,
    expected_kwargs={"source": b"a", "destination": b"b"},
)
@Parametrization.case(
    name="",
    parameters=b"ID 1".split(),
    command_cls=ClientKill,
    expected_kwargs={"client_id": 1},
)
@Parametrization.case(
    name="",
    parameters=b"set-active-expire 0".split(),
    command_cls=Debug,
    expected_kwargs={"set_active_expire": 0},
)
@Parametrization.case(
    name="ping_without_parameters",
    parameters=[],
    command_cls=Ping,
    expected_kwargs={},
)
def test_parser__successful(parameters, command_cls: Command, expected_kwargs: dict[str, Any]):
    actual_kwargs = command_cls.parse(parameters)
    assert actual_kwargs == expected_kwargs


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="",
    parameters=[b"a", b"a", b"2"],
    expected_exception=ValkeySyntaxError,
    command_cls=ByteIntCommand,
)
@Parametrization.case(
    name="",
    parameters=[b"a", b"1"],
    expected_exception=ServerWrongNumberOfArgumentsError,
    command_cls=ByteIntCommand,
)
@Parametrization.case(
    name="",
    parameters=b"myzset NX XX 2 two 3 three".split(),
    expected_exception=ValkeySyntaxError,
    command_cls=SortedSetAdd,
)
def test_parser__failure(parameters, expected_exception, command_cls: Command):
    with pytest.raises(expected_exception):
        command_cls.parse(parameters)
