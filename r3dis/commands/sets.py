import functools
from typing import Callable

from r3dis.commands.databases import DatabaseCommand
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.databases import Database


@RedisCommandsRouter.command(b"smove", [b"write", b"set", b"fast"])
class SetMove(DatabaseCommand):
    source: bytes = redis_positional_parameter()
    destination: bytes = redis_positional_parameter()
    member: bytes = redis_positional_parameter()

    def execute(self):
        source_set = self.database.get_set(self.source)
        destination_set = self.database.get_or_create_set(self.destination)
        if self.member not in source_set:
            return False
        source_set.remove(self.member)
        destination_set.add(self.member)
        return True


@RedisCommandsRouter.command(b"smismember", [b"read", b"set", b"slow"])
class SetAreMembers(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    members: set[bytes] = redis_positional_parameter()

    def handle(self):
        a_set = self.database.get_set(self.key)
        return list(map(lambda m: m in a_set, self.members))


@RedisCommandsRouter.command(b"sismember", [b"read", b"set", b"fast"])
class SetIsMember(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    member: bytes = redis_positional_parameter()

    def execute(self):
        return self.member in self.database.get_set(self.key)


@RedisCommandsRouter.command(b"smembers", [b"read", b"set", b"fast"])
class SetMembers(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return list(self.database.get_set(self.key))


@RedisCommandsRouter.command(b"scard", [b"read", b"set", b"fast"])
class SetCardinality(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return len(self.database.get_set(self.key))


@RedisCommandsRouter.command(b"sadd", [b"write", b"set", b"fast"])
class SetAdd(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    members: set[bytes] = redis_positional_parameter()

    def execute(self):
        a_set = self.database.get_or_create_set(self.key)
        length_before = len(a_set)
        for member in self.members:
            a_set.add(member)
        return len(a_set) - length_before


@RedisCommandsRouter.command(b"spop", [b"write", b"set", b"fast"])
class SetPop(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    count: int = redis_positional_parameter(default=None)

    def execute(self):
        a_set = self.database.get_set(self.key).pop()
        if self.count is None:
            return a_set.pop() if a_set else None
        return [a_set.pop() for _ in range(min(len(a_set), self.count))]


@RedisCommandsRouter.command(b"srem", [b"write", b"set", b"fast"])
class SetRemove(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    members: set[bytes] = redis_positional_parameter()

    def execute(self):
        a_set = self.database.get_set(self.key)
        self.database[self.key] = a_set - self.members
        return len(a_set.intersection(self.members))


def apply_set_operation(database: Database, operation: Callable[[set, set], set], keys: list[bytes]):
    return list(functools.reduce(operation, map(database.get_set, keys)))  # type: ignore


@RedisCommandsRouter.command(b"sunion", [b"read", b"set", b"slow"])
class SetUnion(DatabaseCommand):
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return apply_set_operation(self.database, set.union, self.keys)


@RedisCommandsRouter.command(b"sinter", [b"read", b"set", b"slow"])
class SetIntersection(DatabaseCommand):
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return apply_set_operation(self.database, set.intersection, self.keys)


@RedisCommandsRouter.command(b"sdiff", [b"read", b"set", b"slow"])
class SetDifference(DatabaseCommand):
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return apply_set_operation(self.database, set.difference, self.keys)


def apply_set_store_operation(
    database: Database, operation: Callable[[set, set], set], keys: list[bytes], destination: bytes
):
    database[destination] = functools.reduce(operation, map(database.get_set, keys))
    return len(database[destination])


@RedisCommandsRouter.command(b"sunionstore", [b"write", b"set", b"slow"])
class SetUnionStore(DatabaseCommand):
    destination: bytes = redis_positional_parameter()
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return apply_set_store_operation(self.database, set.union, self.keys, self.destination)


@RedisCommandsRouter.command(b"sinterstore", [b"write", b"set", b"slow"])
class SetIntersectionStore(DatabaseCommand):
    destination: bytes = redis_positional_parameter()
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return apply_set_store_operation(self.database, set.intersection, self.keys, self.destination)


@RedisCommandsRouter.command(b"sdiffstore", [b"write", b"set", b"slow"])
class SetDifferenceStore(DatabaseCommand):
    destination: bytes = redis_positional_parameter()
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return apply_set_store_operation(self.database, set.difference, self.keys, self.destination)
