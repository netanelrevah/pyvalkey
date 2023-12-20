from enum import Enum

from r3dis.commands.databases import DatabaseCommand
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.commands.utils import parse_range_parameters


@RedisCommandsRouter.command(b"llen", [b"read", b"list", b"fast"])
class ListLength(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return len(self.database.get_list(self.key))


@RedisCommandsRouter.command(b"lindex", [b"read", b"list", b"slow"])
class ListIndex(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    index: int = redis_positional_parameter()

    def execute(self):
        redis_list = self.database.get_list(self.key)

        try:
            return redis_list[self.index]
        except IndexError:
            return None


@RedisCommandsRouter.command(b"lrange", [b"read", b"list", b"slow"])
class ListRange(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    start: int = redis_positional_parameter()
    stop: int = redis_positional_parameter()

    def execute(self):
        return self.database.get_list(self.key)[parse_range_parameters(self.start, self.stop)]


class DirectionMode(Enum):
    BEFORE = b"before"
    AFTER = b"after"


@RedisCommandsRouter.command(b"linsert", [b"write", b"list", b"slow"])
class ListInsert(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    direction: DirectionMode = redis_positional_parameter()
    pivot: bytes = redis_positional_parameter()
    element: bytes = redis_positional_parameter()

    def execute(self):
        a_list = self.database.get_or_create_list(self.key)

        if not a_list:
            return 0
        try:
            index = a_list.index(self.pivot)
        except ValueError:
            return -1
        a_list.insert(index + 0 if self.direction == DirectionMode.BEFORE else 1, self.element)
        return len(a_list)


@RedisCommandsRouter.command(b"lpush", [b"write", b"list", b"fast"])
class ListPush(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    values: list[bytes] = redis_positional_parameter()

    def execute(self):
        a_list = self.database.get_or_create_list(self.key)

        for v in self.values:
            a_list.insert(0, v)
        return len(a_list)


@RedisCommandsRouter.command(b"rpush", [b"write", b"list", b"fast"])
class ListPushAtTail(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    values: list[bytes] = redis_positional_parameter()

    def execute(self):
        a_list = self.database.get_or_create_list(self.key)

        for v in self.values:
            a_list.append(v)
        return len(a_list)


@RedisCommandsRouter.command(b"lpop", [b"write", b"list", b"fast"])
class ListPop(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    count: int = redis_positional_parameter(default=None)

    def execute(self):
        a_list = self.database.get_or_create_list(self.key)

        if not a_list:
            return None
        if self.count:
            return [a_list.pop(0) for _ in range(min(len(a_list), self.count))]
        return a_list.pop(0)


@RedisCommandsRouter.command(b"lrem", [b"write", b"list", b"slow"])
class ListRemove(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    count: int = redis_positional_parameter()
    element: bytes = redis_positional_parameter()

    def execute(self):
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
