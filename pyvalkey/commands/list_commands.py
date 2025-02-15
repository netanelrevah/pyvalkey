from enum import Enum

from pyvalkey.commands.consts import LONG_MAX
from pyvalkey.commands.core import AsyncCommand
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter, valkey_command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ArrayNone, ValueType


class DirectionMode(Enum):
    BEFORE = b"before"
    AFTER = b"after"


@ServerCommandsRouter.command(b"blpop", [b"write", b"list", b"fast"])
class ListBlockingLeftPop(AsyncCommand):
    database: Database = server_command_dependency()

    keys: list[bytes] = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"lindex", [b"read", b"list", b"slow"])
class ListIndex(DatabaseCommand):
    key: bytes = positional_parameter()
    index: int = positional_parameter()

    def execute(self) -> ValueType:
        server_list = self.database.get_list(self.key)

        try:
            return server_list[self.index]
        except IndexError:
            return None


@ServerCommandsRouter.command(b"linsert", [b"write", b"list", b"slow"])
class ListInsert(DatabaseCommand):
    key: bytes = positional_parameter()
    direction: DirectionMode = positional_parameter()
    pivot: bytes = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

        if not a_list:
            return 0
        try:
            index = a_list.index(self.pivot)
        except ValueError:
            return -1
        a_list.insert(index + 0 if self.direction == DirectionMode.BEFORE else 1, self.element)
        return len(a_list)


@ServerCommandsRouter.command(b"llen", [b"read", b"list", b"fast"])
class ListLength(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.get_list(self.key))


@ServerCommandsRouter.command(b"lrange", [b"read", b"list", b"slow"])
class ListRange(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    stop: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.get_list(self.key)[parse_range_parameters(self.start, self.stop)]


@ServerCommandsRouter.command(b"lpop", [b"write", b"list", b"fast"])
class ListPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.count is not None and self.count < 0:
            raise ServerError(b"ERR value is out of range, must be positive")

        a_list = self.database.get_or_create_list(self.key)

        if not a_list:
            return None if (self.count is None) else ArrayNone
        if self.count is not None:
            return [a_list.pop(0) for _ in range(min(len(a_list), self.count))]
        return a_list.pop(0)


@valkey_command(b"lpos", [b"read", b"list", b"slow"])
class ListPosition(DatabaseCommand):
    key: bytes = positional_parameter()
    element: bytes = positional_parameter()
    rank: int | None = keyword_parameter(token=b"RANK", default=None)
    number_of_matches: int | None = keyword_parameter(token=b"COUNT", default=None)
    maximum_length: int | None = keyword_parameter(token=b"MAXLEN", default=None)

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

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


@ServerCommandsRouter.command(b"lpush", [b"write", b"list", b"fast"])
class ListPush(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"W")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

        for v in self.values:
            a_list.insert(0, v)
        return len(a_list)


@ServerCommandsRouter.command(b"rpop", [b"write", b"list", b"fast"])
class ListRightPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.count is not None and self.count < 0:
            raise ServerError(b"ERR value is out of range, must be positive")

        a_list = self.database.get_or_create_list(self.key)

        if not a_list:
            return None if (self.count is None) else ArrayNone
        if self.count is not None:
            return [a_list.pop(-1) for _ in range(min(len(a_list), self.count))]
        return a_list.pop(-1)


@ServerCommandsRouter.command(b"lrem", [b"write", b"list", b"slow"])
class ListRemove(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

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


@ServerCommandsRouter.command(b"rpush", [b"write", b"list", b"fast"])
class ListPushAtTail(DatabaseCommand):
    key: bytes = positional_parameter()
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

        for v in self.values:
            a_list.append(v)
        return len(a_list)
