import functools
from typing import Callable, List, Set

from pyvalkey.commands.databases import DatabaseCommand
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"smove", [b"write", b"set", b"fast"])
class SetMove(DatabaseCommand):
    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        source_set = self.database.get_set(self.source)
        destination_set = self.database.get_or_create_set(self.destination)
        if self.member not in source_set:
            return False
        source_set.remove(self.member)
        destination_set.add(self.member)
        return True


@ServerCommandsRouter.command(b"smismember", [b"read", b"set", b"slow"])
class SetAreMembers(DatabaseCommand):
    key: bytes = positional_parameter()
    members: Set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_set = self.database.get_set(self.key)
        return list(map(lambda m: m in a_set, self.members))


@ServerCommandsRouter.command(b"sismember", [b"read", b"set", b"fast"])
class SetIsMember(DatabaseCommand):
    key: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.member in self.database.get_set(self.key)


@ServerCommandsRouter.command(b"smembers", [b"read", b"set", b"fast"])
class SetMembers(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.get_set(self.key))


@ServerCommandsRouter.command(b"scard", [b"read", b"set", b"fast"])
class SetCardinality(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.get_set(self.key))


@ServerCommandsRouter.command(b"sadd", [b"write", b"set", b"fast"])
class SetAdd(DatabaseCommand):
    key: bytes = positional_parameter()
    members: Set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_set = self.database.get_or_create_set(self.key)
        length_before = len(a_set)
        for member in self.members:
            a_set.add(member)
        return len(a_set) - length_before


@ServerCommandsRouter.command(b"spop", [b"write", b"set", b"fast"])
class SetPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter(default=None)

    def execute(self) -> ValueType:
        a_set = self.database.get_set(self.key).pop()
        if self.count is None:
            return a_set.pop() if a_set else None
        return [a_set.pop() for _ in range(min(len(a_set), self.count))]


@ServerCommandsRouter.command(b"srem", [b"write", b"set", b"fast"])
class SetRemove(DatabaseCommand):
    key: bytes = positional_parameter()
    members: Set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_set = self.database.get_set(self.key)
        self.database[self.key] = a_set - self.members
        return len(a_set.intersection(self.members))


def apply_set_operation(database: Database, operation: Callable[[set, set], set], keys: List[bytes]) -> list:
    return list(functools.reduce(operation, map(database.get_set, keys)))  # type: ignore[arg-type]


@ServerCommandsRouter.command(b"sunion", [b"read", b"set", b"slow"])
class SetUnion(DatabaseCommand):
    keys: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.union, self.keys)


@ServerCommandsRouter.command(b"sinter", [b"read", b"set", b"slow"])
class SetIntersection(DatabaseCommand):
    keys: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.intersection, self.keys)


@ServerCommandsRouter.command(b"sdiff", [b"read", b"set", b"slow"])
class SetDifference(DatabaseCommand):
    keys: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.difference, self.keys)


def apply_set_store_operation(
    database: Database, operation: Callable[[set, set], set], keys: List[bytes], destination: bytes
) -> int:
    database[destination] = functools.reduce(operation, map(database.get_set, keys))
    return len(database[destination])


@ServerCommandsRouter.command(b"sunionstore", [b"write", b"set", b"slow"])
class SetUnionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.union, self.keys, self.destination)


@ServerCommandsRouter.command(b"sinterstore", [b"write", b"set", b"slow"])
class SetIntersectionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.intersection, self.keys, self.destination)


@ServerCommandsRouter.command(b"sdiffstore", [b"write", b"set", b"slow"])
class SetDifferenceStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.difference, self.keys, self.destination)
