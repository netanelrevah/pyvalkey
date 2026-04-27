from __future__ import annotations

import fnmatch
import functools
import random
from typing import TYPE_CHECKING

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.consts import LONG_MAX
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.enums import NotificationType
from pyvalkey.utils.dependencies import dependency

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from pyvalkey.database_objects.databases import Database
    from pyvalkey.resp import ValueType


@command(b"smove", {b"set"}, flags={b"fast", b"write"})
class SetMove(Command):
    """
    summary: Moves a member from one set to another.
    complexity: O(1)
    since: 1.0.0
    function: smoveCommand
    reply_schema:
      oneOf:
      - const: 1
        description: Element is moved.
      - const: 0
        description: The element is not a member of source and no operation was performed.
    """

    database: Database = dependency()
    source: bytes = positional_parameter(key_mode=b"RW")
    destination: bytes = positional_parameter(key_mode=b"RW")
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


@command(b"smismember", {b"read", b"set"}, flags={b"fast", b"readonly"})
class SetAreMembers(Command):
    """
    summary: Determines whether multiple members belong to a set.
    complexity: O(N) where N is the number of elements being checked for membership
    since: 6.2.0
    function: smismemberCommand
    reply_schema:
      type: array
      description: >-
        List representing the membership of the given elements, in the same order as they are requested.
      minItems: 1
      items:
        oneOf:
        - const: 0
          description: Not a member of the set or the key does not exist.
        - const: 1
          description: A member of the set.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    members: list[bytes] = positional_parameter(sequence_allow_empty=False)

    def execute(self) -> ValueType:
        a_set = self.database.set_database.get_value_or_empty(self.key)
        return list(map(lambda m: m in a_set, self.members))


@command(b"sismember", {b"read", b"set"}, flags={b"fast", b"readonly"})
class SetIsMember(Command):
    """
    summary: Determines whether a member belongs to a set.
    complexity: O(1)
    since: 1.0.0
    function: sismemberCommand
    reply_schema:
      oneOf:
      - const: 0
        description: The element is not a member of the set, or the key does not exist.
      - const: 1
        description: The element is a member of the set.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.member in self.database.set_database.get_value_or_empty(self.key)


@command(b"smembers", {b"read", b"set", b"slow"}, flags={b"readonly"})
class SetMembers(Command):
    """
    summary: Returns all members of a set.
    complexity: O(N) where N is the set cardinality.
    since: 1.0.0
    function: sinterCommand
    reply_schema:
      type: array
      description: All elements of the set.
      uniqueItems: true
      items:
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return list(self.database.set_database.get_value_or_empty(self.key))


@command(b"scard", {b"read", b"set"}, flags={b"fast", b"readonly"})
class SetCardinality(Command):
    """
    summary: Returns the number of members in a set.
    complexity: O(1)
    since: 1.0.0
    function: scardCommand
    reply_schema:
      description: The cardinality (number of elements) of the set, or 0 if key does not exist.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return len(self.database.set_database.get_value_or_empty(self.key))


@command(b"sadd", {b"set"}, flags={b"denyoom", b"fast", b"write"})
class SetAdd(Command):
    """
    summary: Adds one or more members to a set. Creates the key if it doesn't exist.
    complexity: >-
      O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    since: 1.0.0
    function: saddCommand
    reply_schema:
      description: >-
        Number of elements that were added to the set, not including all the elements already present in the set.
      type: integer
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
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


@command(b"spop", {b"set"}, flags={b"fast", b"write"})
class SetPop(Command):
    """
    summary: >-
      Returns one or more random members from a set after removing them. Deletes the set if the last member was
      popped.
    complexity: >-
      Without the count argument O(1), otherwise O(N) where N is the value of the passed count.
    since: 1.0.0
    function: spopCommand
    reply_schema:
      oneOf:
      - type: 'null'
        description: The key does not exist.
      - type: string
        description: The removed member when 'COUNT' is not given.
      - type: array
        description: List to the removed members when 'COUNT' is given.
        uniqueItems: true
        items:
          type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    count: int = positional_parameter(default=None)

    def execute(self) -> ValueType:
        value = self.database.set_database.get_value_or_none(self.key)
        if value is None:
            return None
        if self.count is None:
            return value.pop() if value else None
        return [value.pop() for _ in range(min(len(value), self.count))]


@command(b"srem", {b"set"}, flags={b"fast", b"write"})
class SetRemove(Command):
    """
    summary: >-
      Removes one or more members from a set. Deletes the set if the last member was removed.
    complexity: O(N) where N is the number of members to be removed.
    since: 1.0.0
    function: sremCommand
    reply_schema:
      description: >-
        Number of members that were removed from the set, not including non existing members.
      type: integer
      minimum: 0
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
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


@command(b"sunion", {b"read", b"set", b"slow"}, flags={b"readonly"})
class SetUnion(Command):
    """
    summary: Returns the union of multiple sets.
    complexity: O(N) where N is the total number of elements in all given sets.
    since: 1.0.0
    function: sunionCommand
    reply_schema:
      type: array
      description: List with the members of the resulting set.
      uniqueItems: true
      items:
        type: string
    """

    database: Database = dependency()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.union, self.keys)


@command(b"sinter", {b"read", b"set", b"slow"}, flags={b"readonly"})
class SetIntersection(Command):
    """
    summary: Returns the intersect of multiple sets.
    complexity: >-
      O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
    since: 1.0.0
    function: sinterCommand
    reply_schema:
      type: array
      description: List with the members of the resulting set.
      uniqueItems: true
      items:
        type: string
    """

    database: Database = dependency()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, set.intersection, self.keys)


@command(
    b"sintercard",
    {b"read", b"set", b"slow"},
    flags={b"readonly"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class SetIntersectionCardinality(Command):
    """
    summary: Returns the number of members of the intersect of multiple sets.
    complexity: >-
      O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
    since: 7.0.0
    function: sinterCardCommand
    reply_schema:
      description: Number of the elements in the resulting intersection.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
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


@command(b"sdiff", {b"read", b"set", b"slow"}, flags={b"readonly"})
class SetDifference(Command):
    """
    summary: Returns the difference of multiple sets.
    complexity: O(N) where N is the total number of elements in all given sets.
    since: 1.0.0
    function: sdiffCommand
    reply_schema:
      type: array
      description: List with the members of the resulting set.
      uniqueItems: true
      items:
        type: string
    """

    database: Database = dependency()
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


@command(b"sunionstore", {b"set", b"slow"}, flags={b"denyoom", b"write"})
class SetUnionStore(Command):
    """
    summary: Stores the union of multiple sets in a key.
    complexity: O(N) where N is the total number of elements in all given sets.
    since: 1.0.0
    function: sunionstoreCommand
    reply_schema:
      type: integer
      description: Number of the elements in the resulting set.
      minimum: 0
    """

    database: Database = dependency()
    destination: bytes = positional_parameter(key_mode=b"W")
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.union, self.keys, self.destination)


@command(b"sinterstore", {b"set", b"slow"}, flags={b"denyoom", b"write"})
class SetIntersectionStore(Command):
    """
    summary: Stores the intersect of multiple sets in a key.
    complexity: >-
      O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
    since: 1.0.0
    function: sinterstoreCommand
    reply_schema:
      description: Number of the elements in the result set.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    destination: bytes = positional_parameter(key_mode=b"W")
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.intersection, self.keys, self.destination)


@command(b"sdiffstore", {b"set", b"slow"}, flags={b"denyoom", b"write"})
class SetDifferenceStore(Command):
    """
    summary: Stores the difference of multiple sets in a key.
    complexity: O(N) where N is the total number of elements in all given sets.
    since: 1.0.0
    function: sdiffstoreCommand
    reply_schema:
      description: Number of the elements in the resulting set.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    destination: bytes = positional_parameter(key_mode=b"W")
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, set.difference, self.keys, self.destination)


@command(b"srandmember", {b"read", b"set", b"slow"}, flags={b"readonly"})
class SetRandomMember(Command):
    """
    summary: Gets one or multiple random members from a set.
    complexity: >-
      Without the count argument O(1), otherwise O(N) where N is the absolute value of the passed count.
    since: 1.0.0
    function: srandmemberCommand
    reply_schema:
      oneOf:
      - description: In case `count` is not given and key doesn't exist
        type: 'null'
      - description: In case `count` is not given, randomly selected element
        type: string
      - description: In case `count` is given, an array of elements
        type: array
        items:
          type: string
        minItems: 1
      - description: In case `count` is given and key doesn't exist
        type: array
        maxItems: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
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


@command(b"sscan", {b"read", b"set", b"slow"}, flags={b"readonly"})
class SetScan(Command):
    """
    summary: Iterates over members of a set.
    complexity: >-
      O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return
      back to 0. N is the number of elements inside the collection.
    since: 2.8.0
    function: sscanCommand
    reply_schema:
      description: Cursor and scan response in array form.
      type: array
      minItems: 2
      maxItems: 2
      items:
      - description: Cursor.
        type: string
      - description: List of set members.
        type: array
        items:
          type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    cursor: int = positional_parameter()
    match: bytes | None = keyword_parameter(token=b"MATCH", default=None)
    count: int | None = keyword_parameter(token=b"COUNT", default=None)

    def execute(self) -> ValueType:
        value = self.database.set_database.get_value_or_empty(self.key)

        def scan() -> Iterable[bytes]:
            for item in value:
                if self.match is not None and not fnmatch.fnmatch(item, self.match):
                    continue
                yield item

        return [b"0", [item for item in scan()]]
