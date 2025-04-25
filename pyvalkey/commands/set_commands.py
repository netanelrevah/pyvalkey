import functools
import random
from collections.abc import Callable

from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import ValueType


@command(b"smove", [b"write", b"set", b"fast"])
class SetMove(DatabaseCommand):
    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        with (
            self.database.set_database.get_with_context(self.source) as source_set,
            self.database.set_database.get_or_create_with_context(self.destination) as destination_set,
        ):
            if self.member not in source_set.value:
                return False
            source_set.value.remove(self.member)
            destination_set.value.add(self.member)
            return True


@command(b"smismember", [b"read", b"set", b"slow"])
class SetAreMembers(DatabaseCommand):
    key: bytes = positional_parameter()
    members: set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_set = self.database.set_database.get(self.key)
        return list(map(lambda m: m in a_set.value, self.members))


@command(b"sismember", [b"read", b"set", b"fast"])
class SetIsMember(DatabaseCommand):
    key: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.member in self.database.set_database.get(self.key).value


@command(b"smembers", [b"read", b"set", b"fast"])
class SetMembers(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.set_database.get(self.key).value)


@command(b"scard", [b"read", b"set", b"fast"])
class SetCardinality(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.set_database.get(self.key).value)


@command(b"sadd", [b"write", b"set", b"fast"])
class SetAdd(DatabaseCommand):
    key: bytes = positional_parameter()
    members: set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.set_database.get_or_create(self.key)
        length_before = len(key_value.value)
        for member in self.members:
            key_value.value.add(member)
        return len(key_value.value) - length_before


@command(b"spop", [b"write", b"set", b"fast"])
class SetPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter(default=None)

    def execute(self) -> ValueType:
        with self.database.set_database.get_value_with_context(self.key) as value:
            if self.count is None:
                return value.pop() if value else None
            return [value.pop() for _ in range(min(len(value), self.count))]


@command(b"srem", [b"write", b"set", b"fast"])
class SetRemove(DatabaseCommand):
    key: bytes = positional_parameter()
    members: set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_set = self.database.set_database.get(self.key).value
        a_set.difference_update(self.members)
        return len(a_set.intersection(self.members))


def apply_set_operation(database: Database, operation: Callable[[set, set], set], keys: list[bytes]) -> list:
    return list(functools.reduce(operation, map(lambda x: database.set_database.get(x).value, keys)))  # type: ignore[arg-type]


@command(b"sunion", [b"read", b"set", b"slow"])
class SetUnion(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.union, self.keys)


@command(b"sinter", [b"read", b"set", b"slow"])
class SetIntersection(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.intersection, self.keys)


@command(b"sdiff", [b"read", b"set", b"slow"])
class SetDifference(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.difference, self.keys)


def apply_set_store_operation(
    database: Database, operation: Callable[[set, set], set], keys: list[bytes], destination: bytes
) -> int:
    new_set: set = functools.reduce(operation, map(lambda x: database.set_database.get(x).value, keys))
    database.pop(destination, None)
    database.set_database.get_or_create(destination).value.update(new_set)
    return len(new_set)


@command(b"sunionstore", [b"write", b"set", b"slow"])
class SetUnionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.union, self.keys, self.destination)


@command(b"sinterstore", [b"write", b"set", b"slow"])
class SetIntersectionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.intersection, self.keys, self.destination)


@command(b"sdiffstore", [b"write", b"set", b"slow"])
class SetDifferenceStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.difference, self.keys, self.destination)


@command(b"srandmember", [b"write", b"string", b"slow"])
class SetRandomMember(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        key_value = self.database.set_database.get_or_none(self.key)
        s: set[bytes] | None = None
        if key_value is not None:
            s = key_value.value

        if self.count is None:
            if s is None:
                return None
            return random.choice(list(s))

        if s is None:
            return []

        items = list(s)

        if self.count < 0:
            return [random.choice(items) for _ in range(abs(self.count))]

        result = []
        for _ in range(self.count):
            if not items:
                break

            result.append(items.pop(random.randrange(len(items))))
        return result
