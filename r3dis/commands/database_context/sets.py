import functools
from dataclasses import dataclass
from typing import Callable

from r3dis.commands.core import redis_argument
from r3dis.commands.database_context.core import DatabaseCommand
from r3dis.databases import Database


@dataclass
class SetMove(DatabaseCommand):
    source: bytes = redis_argument()
    destination: bytes = redis_argument()
    member: bytes = redis_argument()

    def execute(self):
        source_set = self.database.get_set(self.source)
        destination_set = self.database.get_or_create_set(self.destination)
        if self.member not in source_set:
            return False
        source_set.remove(self.member)
        destination_set.add(self.member)
        return True


@dataclass
class SetAreMembers(DatabaseCommand):
    key: bytes
    members: set[bytes]

    def handle(self):
        a_set = self.database.get_set(self.key)
        return list(map(lambda m: m in a_set, self.members))


@dataclass
class SetIsMember(DatabaseCommand):
    key: bytes
    member: bytes

    def execute(self):
        return self.member in self.database.get_set(self.key)


@dataclass
class SetMembers(DatabaseCommand):
    key: bytes = redis_argument()

    def execute(self):
        return list(self.database.get_set(self.key))


@dataclass
class SetCardinality(DatabaseCommand):
    key: bytes = redis_argument()

    def execute(self):
        return len(self.database.get_set(self.key))


@dataclass
class SetAdd(DatabaseCommand):
    key: bytes = redis_argument()
    members: set[bytes] = redis_argument()

    def execute(self):
        a_set = self.database.get_or_create_set(self.key)
        length_before = len(a_set)
        for member in self.members:
            a_set.add(member)
        return len(a_set) - length_before


@dataclass
class SetPop(DatabaseCommand):
    key: bytes = redis_argument()
    count: int = redis_argument(default=None)

    def execute(self):
        a_set = self.database.get_set(self.key).pop()
        if self.count is None:
            return a_set.pop() if a_set else None
        return [a_set.pop() for _ in range(min(len(a_set), self.count))]


@dataclass
class SetRemove(DatabaseCommand):
    key: bytes = redis_argument()
    members: set[bytes] = redis_argument()

    def execute(self):
        a_set = self.database.get_set(self.key)
        self.database[self.key] = a_set - self.members
        return len(a_set.intersection(self.members))


def apply_set_operation(database: Database, operation: Callable[[set, set], set], keys: list[bytes]):
    return list(functools.reduce(operation, map(database.get_set, keys)))


@dataclass
class SetUnion(DatabaseCommand):
    keys: list[bytes]

    def execute(self):
        return apply_set_operation(self.database, set.union, self.keys)


@dataclass
class SetIntersection(DatabaseCommand):
    keys: list[bytes]

    def execute(self):
        return apply_set_operation(self.database, set.intersection, self.keys)


@dataclass
class SetDifference(DatabaseCommand):
    keys: list[bytes]

    def execute(self):
        return apply_set_operation(self.database, set.difference, self.keys)


def apply_set_store_operation(
    database: Database, operation: Callable[[set, set], set], keys: list[bytes], destination: bytes
):
    database[destination] = functools.reduce(operation, map(database.get_set, keys))
    return len(database[destination])


@dataclass
class SetUnionStore(DatabaseCommand):
    destination: bytes = redis_argument()
    keys: list[bytes] = redis_argument()

    def execute(self):
        return apply_set_store_operation(self.database, set.union, self.keys, self.destination)


@dataclass
class SetIntersectionStore(DatabaseCommand):
    destination: bytes = redis_argument()
    keys: list[bytes] = redis_argument()

    def execute(self):
        return apply_set_store_operation(self.database, set.intersection, self.keys, self.destination)


@dataclass
class SetDifferenceStore(DatabaseCommand):
    destination: bytes = redis_argument()
    keys: list[bytes] = redis_argument()

    def execute(self):
        return apply_set_store_operation(self.database, set.difference, self.keys, self.destination)
