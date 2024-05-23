from enum import Enum

from pyvalkey.commands.databases import DatabaseCommand
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"llen", [b"read", b"list", b"fast"])
class ListLength(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.get_list(self.key))


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


@ServerCommandsRouter.command(b"lrange", [b"read", b"list", b"slow"])
class ListRange(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    stop: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.get_list(self.key)[parse_range_parameters(self.start, self.stop)]


class DirectionMode(Enum):
    BEFORE = b"before"
    AFTER = b"after"


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


@ServerCommandsRouter.command(b"lpush", [b"write", b"list", b"fast"])
class ListPush(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"W")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

        for v in self.values:
            a_list.insert(0, v)
        return len(a_list)


@ServerCommandsRouter.command(b"rpush", [b"write", b"list", b"fast"])
class ListPushAtTail(DatabaseCommand):
    key: bytes = positional_parameter()
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

        for v in self.values:
            a_list.append(v)
        return len(a_list)


@ServerCommandsRouter.command(b"lpop", [b"write", b"list", b"fast"])
class ListPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter(default=None)

    def execute(self) -> ValueType:
        a_list = self.database.get_or_create_list(self.key)

        if not a_list:
            return None
        if self.count:
            return [a_list.pop(0) for _ in range(min(len(a_list), self.count))]
        return a_list.pop(0)


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
