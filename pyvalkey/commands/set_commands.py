from __future__ import annotations

import functools
import random
from collections.abc import Callable

from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.consts import LONG_MAX
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.enums import NotificationType
from pyvalkey.resp import ValueType


@command(b"smove", {b"write", b"set", b"fast"})
class SetMove(DatabaseCommand):
    source: bytes = positional_parameter()
    destination: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        source_set = self.database.set_database.get_value_or_none(self.source)
        if source_set is None:
            return False
        destination_set = self.database.set_database.get_value_or_create(self.destination)

        if self.member not in source_set:
            return False
        source_set.remove(self.member)
        destination_set.add(self.member)
        return True


@command(b"smismember", {b"read", b"set", b"slow"})
class SetAreMembers(DatabaseCommand):
    key: bytes = positional_parameter()
    members: list[bytes] = positional_parameter(sequence_allow_empty=False)

    def execute(self) -> ValueType:
        a_set = self.database.set_database.get_value_or_empty(self.key)
        return list(map(lambda m: m in a_set, self.members))


@command(b"sismember", {b"read", b"set", b"fast"})
class SetIsMember(DatabaseCommand):
    key: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.member in self.database.set_database.get_value_or_empty(self.key)


@command(b"smembers", {b"read", b"set", b"fast"})
class SetMembers(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.set_database.get_value_or_empty(self.key))


@command(b"scard", {b"read", b"set", b"fast"})
class SetCardinality(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.set_database.get_value_or_empty(self.key))


@command(b"sadd", {b"write", b"set", b"fast"})
class SetAdd(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
    members: set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.set_database.get_or_create(self.key)
        length_before = len(key_value.value)
        for member in self.members:
            key_value.value.add(member)
        added = len(key_value.value) - length_before
        if added:
            self.database.notify(NotificationType.SET, b"sadd", self.key)
        return added


@command(b"spop", {b"write", b"set", b"fast"})
class SetPop(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter(default=None)

    def execute(self) -> ValueType:
        value = self.database.set_database.get_value_or_none(self.key)
        if value is None:
            return None
        if self.count is None:
            return value.pop() if value else None
        return [value.pop() for _ in range(min(len(value), self.count))]


@command(b"srem", {b"write", b"set", b"fast"})
class SetRemove(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
    members: set[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.set_database.get_value(self.key)
        to_remove = len(self.members.intersection(value))
        value.difference_update(self.members)

        if to_remove:
            self.database.notify(NotificationType.SET, b"srem", self.key)
            if len(value) == 0:
                self.database.notify(NotificationType.GENERIC, b"del", self.key)

        return to_remove


def apply_set_operation(database: Database, operation: Callable[[set, set], set], keys: list[bytes]) -> list:
    return list(functools.reduce(operation, map(database.set_database.get_value_or_empty, keys)))  # type: ignore[arg-type]


@command(b"sunion", {b"read", b"set", b"slow"})
class SetUnion(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.union, self.keys)


@command(b"sinter", {b"read", b"set", b"slow"})
class SetIntersection(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.intersection, self.keys)


@command(
    b"sintercard", {b"read", b"set", b"fast"}, metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"}
)
class SetIntersectionCardinality(DatabaseCommand):
    numkeys: int = positional_parameter(parse_error=b"ERR numkeys should be greater than 0")
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={
            "when_length_field_more_then_parameters": b"ERR Number of keys can't be greater than number of args",
        },
    )
    limit: int = keyword_parameter(token=b"LIMIT", default=0, parse_error=b"ERR LIMIT can't be negative")

    def execute(self) -> ValueType:
        if self.limit < 0:
            raise ServerError(b"ERR LIMIT can't be negative")

        result_set: set[bytes] = set(self.database.set_database.get_value_or_empty(self.keys[0]))
        for key in self.keys[1:]:
            result_set.intersection_update(self.database.set_database.get_value_or_empty(key))
            if 0 < self.limit <= len(result_set):
                return self.limit
        return len(result_set)


@command(b"sdiff", {b"read", b"set", b"slow"})
class SetDifference(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.difference, self.keys)


def apply_set_store_operation(
    database: Database, operation: Callable[[set, set], set], keys: list[bytes], destination: bytes
) -> int:
    new_set: set = functools.reduce(operation, map(database.set_database.get_value_or_empty, keys))
    database.pop(destination, None)
    database.set_database.get_or_create(destination).value.update(new_set)
    return len(new_set)


@command(b"sunionstore", {b"write", b"set", b"slow"})
class SetUnionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.union, self.keys, self.destination)


@command(b"sinterstore", {b"write", b"set", b"slow"})
class SetIntersectionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.intersection, self.keys, self.destination)


@command(b"sdiffstore", {b"write", b"set", b"slow"})
class SetDifferenceStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.difference, self.keys, self.destination)


@command(b"srandmember", {b"write", b"string", b"slow"})
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

        if not (-LONG_MAX < self.count < LONG_MAX):
            raise ServerError(f"ERR value is out of range, value must between {-LONG_MAX} and {LONG_MAX}".encode())

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
