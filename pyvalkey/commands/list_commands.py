from dataclasses import field
from enum import Enum

from pyvalkey.commands.consts import LONG_MAX
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.database_objects.databases import BlockingManager, Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ArrayNone, ValueType


class DirectionMode(Enum):
    BEFORE = b"BEFORE"
    AFTER = b"AFTER"


@command(b"blpop", {b"write", b"list", b"fast"})
class ListBlockingLeftPop(Command):
    database: Database = server_command_dependency()
    blocking_manager: BlockingManager = server_command_dependency()

    keys: list[bytes] = positional_parameter()
    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.database, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(0)
        return [self._key, value]


@command(b"brpop", {b"write", b"list", b"fast"})
class ListBlockingRightPop(Command):
    database: Database = server_command_dependency()
    blocking_manager: BlockingManager = server_command_dependency()

    keys: list[bytes] = positional_parameter()
    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.database, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(-1)
        return [self._key, value]


class Direction(Enum):
    LEFT = b"LEFT"
    RIGHT = b"RIGHT"


@command(b"blmpop", {b"write", b"list", b"fast"})
class ListBlockingMultiplePop(Command):
    database: Database = server_command_dependency()
    blocking_manager: BlockingManager = server_command_dependency()

    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")
    num_keys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(length_field_name="num_keys")
    direction: Direction = positional_parameter()
    count: int = keyword_parameter(default=1, token=b"COUNT")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.database, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        values = [
            list_value.pop(0 if self.direction == Direction.LEFT else -1)
            for _ in range(min(self.count, len(list_value)))
        ]
        return [self._key, values]


@command(b"lmpop", {b"write", b"list", b"fast"})
class ListMultiplePop(Command):
    database: Database = server_command_dependency()

    numkeys: int = positional_parameter(parse_error=b"ERR numkeys should be greater than 0")
    keys: list[bytes] = positional_parameter(length_field_name="numkeys")
    direction: Direction = positional_parameter()
    count: int = keyword_parameter(default=1, token=b"COUNT", parse_error=b"ERR count should be greater than 0")

    def execute(self) -> ValueType:
        if self.count <= 0:
            raise ServerError(b"ERR count should be greater than 0")

        for key in self.keys:
            value = self.database.list_database.get_value_or_none(key)
            if value is not None:
                break
        else:
            return None

        values = [value.pop(0 if self.direction == Direction.LEFT else -1) for _ in range(min(self.count, len(value)))]
        return [key, values]


@command(b"brpoplpush", {b"write", b"list", b"fast"})
class ListBlockingRightPopLeftPush(Command):
    database: Database = server_command_dependency()
    blocking_manager: BlockingManager = server_command_dependency()

    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()
    timeout: int = positional_parameter()

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.database, [self.source], self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        destination_list = self.database.list_database.get_value_or_create(self.destination)
        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(-1)
        destination_list.insert(0, value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        if self._key is not None:
            await self.blocking_manager.notify_list(self.destination, in_multi=in_multi)


@command(b"blmove", {b"write", b"list", b"fast"})
class ListBlockingMove(Command):
    database: Database = server_command_dependency()
    blocking_manager: BlockingManager = server_command_dependency()

    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()
    source_direction: Direction = positional_parameter()
    destination_direction: Direction = positional_parameter()
    timeout: int = positional_parameter()

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.database, [self.source], self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(0 if self.source_direction == Direction.LEFT else -1)
        if self.destination_direction == Direction.LEFT:
            self.database.list_database.get_or_create(self.destination).value.insert(0, value)
        else:
            self.database.list_database.get_or_create(self.destination).value.append(value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        if self._key is not None:
            await self.blocking_manager.notify_list(self.destination, in_multi=in_multi)


@command(b"lmove", {b"write", b"list", b"fast"})
class ListMove(Command):
    database: Database = server_command_dependency()
    blocking_manager: BlockingManager = server_command_dependency()

    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()
    source_direction: Direction = positional_parameter()
    destination_direction: Direction = positional_parameter()

    def execute(self) -> ValueType:
        source_value = self.database.list_database.get_value_or_none(self.source)

        if source_value is None:
            return None

        destination_value = self.database.list_database.get_value_or_create(self.destination)

        value = source_value.pop(0 if self.source_direction == Direction.LEFT else -1)
        if self.destination_direction == Direction.LEFT:
            destination_value.insert(0, value)
        else:
            destination_value.append(value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_list(self.destination, in_multi=in_multi)


@command(b"lindex", {b"read", b"list", b"slow"})
class ListIndex(DatabaseCommand):
    key: bytes = positional_parameter()
    index: int = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.list_database.get_value_or_none(self.key)

        if value is None or self.index >= len(value):
            return None

        return value[self.index]


@command(b"linsert", {b"write", b"list", b"slow"})
class ListInsert(DatabaseCommand):
    key: bytes = positional_parameter()
    direction: DirectionMode = positional_parameter()
    pivot: bytes = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        list_value = self.database.list_database.get_value_or_none(self.key)

        if list_value is None:
            return 0

        try:
            index = list_value.index(self.pivot)
        except ValueError:
            return -1
        list_value.insert(index + (0 if self.direction == DirectionMode.BEFORE else 1), self.element)
        return len(list_value)


@command(b"lset", {b"write", b"list", b"slow"})
class ListSet(DatabaseCommand):
    key: bytes = positional_parameter()
    index: int = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        list_value = self.database.list_database.get_value_or_none(self.key)

        if list_value is None:
            raise ServerError(b"ERR no such key")

        if self.index >= len(list_value):
            raise ServerError(b"ERR index out of range")

        if self.index < 0:
            list_value[self.index + len(list_value)] = self.element

        list_value[self.index] = self.element
        return RESP_OK


@command(b"llen", {b"read", b"list", b"fast"})
class ListLength(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.list_database.get_value_or_empty(self.key))


@command(b"lrange", {b"read", b"list", b"slow"})
class ListRange(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    stop: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.list_database.get_value_or_empty(self.key)[parse_range_parameters(self.start, self.stop)]


@command(b"ltrim", {b"read", b"list", b"slow"})
class ListTrim(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    stop: int = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.list_database.get_or_none(self.key)

        if key_value is not None:
            key_value.value = key_value.value[parse_range_parameters(self.start, self.stop)]
        return RESP_OK


@command(b"lpop", {b"write", b"list", b"fast"})
class ListPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.count is not None and self.count < 0:
            raise ServerError(b"ERR value is out of range, must be positive")

        list_value = self.database.list_database.get_value_or_empty(self.key)
        if not list_value:
            return None if (self.count is None) else ArrayNone
        if self.count is not None:
            value = [list_value.pop(0) for _ in range(min(len(list_value), self.count))]
        else:
            value = list_value.pop(0)
        return value


@command(b"lpos", {b"read", b"list", b"slow"})
class ListPosition(DatabaseCommand):
    key: bytes = positional_parameter()
    element: bytes = positional_parameter()
    rank: int | None = keyword_parameter(token=b"RANK", default=None)
    number_of_matches: int | None = keyword_parameter(token=b"COUNT", default=None)
    maximum_length: int | None = keyword_parameter(token=b"MAXLEN", default=None)

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_or_create(self.key).value

        if self.rank is not None:
            if self.rank == 0:
                raise ServerError(
                    b"ERR RANK can't be zero: "
                    b"use 1 to start from the first match, "
                    b"2 from the second ... or use negative to start from the end of the list"
                )
            if self.rank < -LONG_MAX:
                raise ServerError(
                    b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"
                )

        indexes = []
        skip = abs(self.rank) - 1 if self.rank is not None else 0
        for index in range(len(a_list)):
            real_index = index
            if self.rank is not None and self.rank < 0:
                real_index = len(a_list) - 1 - index

            if self.maximum_length and index >= self.maximum_length:
                break

            item = a_list[real_index]

            if item != self.element:
                continue

            if skip > 0:
                skip -= 1
                continue

            if self.number_of_matches is None:
                return real_index

            indexes.append(real_index)

            if self.number_of_matches != 0 and len(indexes) >= self.number_of_matches:
                break
        return indexes


@command(b"lpush", {b"list", b"fast"}, flags={b"write", b"denyoom"})
class ListPush(DatabaseCommand):
    blocking_manager: BlockingManager = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"W")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_create(self.key)

        for v in self.values:
            a_list.insert(0, v)
        return len(a_list)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_list(self.key, in_multi=in_multi)


@command(b"lpushx", {b"list", b"fast"}, flags={b"write", b"denyoom"})
class ListPushIfExists(DatabaseCommand):
    blocking_manager: BlockingManager = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"W")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_none(self.key)

        if a_list is None:
            return 0

        for v in self.values:
            a_list.insert(0, v)
        return len(a_list)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_list(self.key, in_multi=in_multi)


@command(b"rpop", {b"write", b"list", b"fast"})
class ListRightPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.count is not None and self.count < 0:
            raise ServerError(b"ERR value is out of range, must be positive")

        a_list = self.database.list_database.get_value_or_create(self.key)

        if not a_list:
            return None if (self.count is None) else ArrayNone
        if self.count is not None:
            return [a_list.pop(-1) for _ in range(min(len(a_list), self.count))]
        return a_list.pop(-1)


@command(b"rpoplpush", {b"write", b"list", b"fast"})
class ListRightPopLeftPush(DatabaseCommand):
    blocking_manager: BlockingManager = server_command_dependency()
    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()

    def execute(self) -> ValueType:
        source_value = self.database.list_database.get_value_or_none(self.source)
        if source_value is None:
            return None

        destination_value = self.database.list_database.get_value_or_create(self.destination)
        value = source_value.pop(-1)
        destination_value.insert(0, value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_list(self.destination, in_multi=in_multi)


@command(b"lrem", {b"write", b"list", b"slow"})
class ListRemove(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_create(self.key)

        if not a_list:
            return 0
        count = int(self.count)
        to_delete = abs(count)
        if count < 0:
            a_list.reverse()

        deleted = 0
        for _ in range(to_delete if to_delete > 0 else a_list.count(self.element)):
            try:
                a_list.remove(self.element)
                deleted += 1
            except ValueError:
                break
        if count < 0:
            a_list.reverse()
        return deleted


@command(b"rpush", {b"write", b"list", b"fast"})
class ListPushAtTail(DatabaseCommand):
    blocking_manager: BlockingManager = server_command_dependency()

    key: bytes = positional_parameter()
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_create(self.key)

        for v in self.values:
            a_list.append(v)
        return len(a_list)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_list(self.key, in_multi=in_multi)


@command(b"rpushx", {b"write", b"list", b"fast"})
class ListPushAtTailIfExists(DatabaseCommand):
    blocking_manager: BlockingManager = server_command_dependency()

    key: bytes = positional_parameter()
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        list_value = self.database.list_database.get_value_or_none(self.key)

        if list_value is None:
            return 0

        for v in self.values:
            list_value.append(v)
        return len(list_value)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_list(self.key, in_multi=in_multi)
