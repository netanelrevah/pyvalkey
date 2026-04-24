from __future__ import annotations

from dataclasses import field
from enum import Enum
from math import isinf, isnan
from typing import TYPE_CHECKING, cast

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import increment_bytes_value_as_float, parse_range_parameters
from pyvalkey.consts import LONG_LONG_MIN, LONG_MAX, LONG_MIN, UINT32_MAX
from pyvalkey.database_objects.databases import KeyValue
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.enums import NotificationType
from pyvalkey.resp import RESP_OK, ValueType
from pyvalkey.utils.dependencies import dependency
from pyvalkey.utils.times import now_ms

if TYPE_CHECKING:
    from pyvalkey.blocking import StreamBlockingManager
    from pyvalkey.database_objects.databases import Database, DatabaseBase


def increment_by_int(database: Database, key: bytes, increment: int = 1) -> int:
    previous_value = database.int_database.get_value_or_none(key) or 0

    if (increment < 0 and previous_value < 0 and increment < LONG_MIN - previous_value) or (
        increment > 0 and previous_value > 0 and increment > LONG_MAX - previous_value
    ):
        raise ServerError(b"ERR increment or decrement would overflow")

    new_value = previous_value + increment
    database.int_database.upsert(key, new_value)
    return new_value


def increment_by_float(database: Database, key: bytes, increment: float = 1) -> bytes:
    previous_value = database.bytes_database.get_value_or_none(key) or b"0"
    new_value = increment_bytes_value_as_float(previous_value, increment)
    database.bytes_database.upsert(key, new_value)
    return new_value


def increment_by(database: Database, key: bytes, increment: int | float = 1) -> bytes | int:
    if isinstance(increment, int):
        return increment_by_int(database, key, increment)
    elif isinstance(increment, float):
        return increment_by_float(database, key, increment)
    else:
        raise ValueError()


@command(b"append", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class Append(Command):
    """
    summary: Appends a string to the value of a key. Creates the key if it doesn't exist.
    complexity: >-
      O(1). The amortized time complexity is O(1) assuming the appended value is small and the already present value
      is of any size, since the dynamic string library used by the server will double the free space available on
      every reallocation.
    since: 2.0.0
    function: appendCommand
    reply_schema:
      type: integer
      description: The length of the string after the append operation.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.bytes_database.get_value_or_empty(self.key)
        new_value = value + self.value
        self.database.bytes_database.upsert(self.key, new_value)
        return len(value)


@command(b"decr", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class Decrement(Command):
    """
    summary: >-
      Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: decrCommand
    reply_schema:
      type: integer
      description: The value of the key after decrementing it.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, -1)


@command(b"decrby", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class DecrementBy(Command):
    """
    summary: >-
      Decrements the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: decrbyCommand
    reply_schema:
      type: integer
      description: The value of the key after decrementing it.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    decrement: int = positional_parameter()

    def execute(self) -> ValueType:
        if self.decrement == LONG_LONG_MIN:
            raise ServerError(b"ERR decrement would overflow")

        return increment_by(self.database, self.key, self.decrement * -1)


@command(b"get", {b"read", b"string"}, flags={b"fast", b"readonly"})
class Get(Command):
    """
    summary: Returns the string value of a key.
    complexity: O(1)
    since: 1.0.0
    function: getCommand
    reply_schema:
      oneOf:
      - description: The value of the key.
        type: string
      - description: Key does not exist.
        type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return self.database.string_database.get_value_or_none(self.key)


@command(b"getdel", {b"string"}, flags={b"fast", b"write"})
class GetDelete(Command):
    """
    summary: Returns the string value of a key after deleting the key.
    complexity: O(1)
    since: 6.2.0
    function: getdelCommand
    reply_schema:
      oneOf:
      - description: The value of the key.
        type: string
      - description: The key does not exist.
        type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        key_value = self.database.string_database.pop(self.key, None)
        if key_value is not None:
            return key_value.value
        return None


@command(
    b"getex",
    {b"string"},
    flags={b"fast", b"write"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class GetExpire(Command):
    """
    summary: Returns the string value of a key after setting its expiration time.
    complexity: O(1)
    since: 6.2.0
    function: getexCommand
    reply_schema:
      oneOf:
      - description: The value of the key.
        type: string
      - description: Key does not exist.
        type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)
    persist: bool = flag_parameter(token=b"PERSIST")

    def execute(self) -> ValueType:
        key_value = self.database.string_database.get_or_none(self.key)
        if key_value is None:
            return None

        fields_names = ["ex", "px", "exat", "pxat", "persist"]

        filled = list(map(bool, [getattr(self, name) for name in fields_names]))
        if filled.count(True) > 1:
            raise ServerError(b"ERR syntax error")

        if True in filled:
            name = fields_names[filled.index(True)]
            value = getattr(self, name)

            if name in ["ex", "px"]:
                self.database.set_expiration_in(self.key, value * (1000 if name == "ex" else 1))
            if name in ["exat", "pxat"]:
                self.database.set_expiration_at(self.key, value * (1000 if name == "exat" else 1))
            if name == "persist":
                self.database.set_persist(self.key)

        return key_value.value


@command(b"getrange", {b"read", b"slow", b"string"}, flags={b"readonly"})
class StringGetRange(Command):
    """
    summary: Returns a substring of the string stored at a key.
    complexity: >-
      O(N) where N is the length of the returned string. The complexity is ultimately determined by the returned
      length, but because creating a substring from an existing string is very cheap, it can be considered O(1)
      for small strings.
    since: 2.4.0
    function: getrangeCommand
    reply_schema:
      type: string
      description: >-
        The substring of the string value stored at key, determined by the offsets start and end (both are inclusive).
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    start: int = positional_parameter()
    end: int = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.bytes_database.get_value_or_empty(self.key)
        return value[parse_range_parameters(self.start, self.end)]


@command(b"getset", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class GetSet(Command):
    """
    summary: Returns the previous string value of a key after setting it to a new value.
    complexity: O(1)
    since: 1.0.0
    function: getsetCommand
    reply_schema:
      oneOf:
      - description: The old value stored at the key.
        type: string
      - description: The key does not exist.
        type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        old_value = self.database.string_database.get_value_or_create(self.key)
        self.database.string_database.upsert(self.key, self.value)
        return old_value


@command(b"incr", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class Increment(Command):
    """
    summary: >-
      Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: incrCommand
    reply_schema:
      description: The value of key after the increment
      type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key)


@command(b"incrby", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class IncrementBy(Command):
    """
    summary: >-
      Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: incrbyCommand
    reply_schema:
      type: integer
      description: The value of the key after incrementing it.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    increment: int = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.increment)


@command(b"incrbyfloat", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class IncrementByFloat(Command):
    """
    summary: >-
      Increments the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.
    complexity: O(1)
    since: 2.6.0
    function: incrbyfloatCommand
    reply_schema:
      type: string
      description: The value of the key after incrementing it.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    increment: float = positional_parameter()

    def execute(self) -> ValueType:
        if isnan(self.increment) or isinf(self.increment):
            raise ServerError(b"ERR increment would produce NaN or Infinity")
        return increment_by(self.database, self.key, self.increment)


@command(b"lcs", {b"read", b"slow", b"string"}, flags={b"readonly"})
class LongestCommonSubsequence(Command):
    """
    summary: Finds the longest common substring.
    complexity: O(N*M) where N and M are the lengths of s1 and s2, respectively
    since: 7.0.0
    function: lcsCommand
    reply_schema:
      oneOf:
      - type: string
        description: The longest common subsequence.
      - type: integer
        description: The length of the longest common subsequence when 'LEN' is given.
      - type: object
        description: >-
          Array with the LCS length and all the ranges in both the strings when 'IDX' is given. In RESP2 this is
          returned as a flat array
        additionalProperties: false
        properties:
          matches:
            type: array
            items:
              type: array
              minItems: 2
              maxItems: 3
              items:
              - type: array
                description: Matched range in the first string.
                minItems: 2
                maxItems: 2
                items:
                  type: integer
              - type: array
                description: Matched range in the second string.
                minItems: 2
                maxItems: 2
                items:
                  type: integer
              additionalItems:
                type: integer
                description: The length of the match when 'WITHMATCHLEN' is given.
          len:
            type: integer
            description: Length of the longest common subsequence.
    """

    database: Database = dependency()
    key1: bytes = positional_parameter(key_mode=b"R")
    key2: bytes = positional_parameter(key_mode=b"R")
    length: bool = flag_parameter(token=b"LEN")
    index: bool = flag_parameter(token=b"IDX")
    min_match_length: int = keyword_parameter(token=b"MINMATCHLEN", default=0)
    with_match_length: bool = flag_parameter(token=b"WITHMATCHLEN")

    def compute_matrix(self, s1: bytes, s2: bytes) -> list[list[int]]:
        matrix = [[0 for _ in range(len(s2) + 1)] for _ in range(len(s1) + 1)]

        for i in range(len(s1) + 1):
            for j in range(len(s2) + 1):
                if i == 0 or j == 0:
                    matrix[i][j] = 0
                elif s1[i - 1] == s2[j - 1]:
                    matrix[i][j] = matrix[i - 1][j - 1] + 1
                else:
                    matrix[i][j] = max(matrix[i - 1][j], matrix[i][j - 1])

        return matrix

    def get_lcs_length(self, matrix: list[list[int]]) -> int:
        return matrix[-2][-2] + 1

    def get_lcs_matches(self, matrix: list[list[int]], s1: bytes, s2: bytes) -> tuple[list, bytes]:
        match_bytes = b""
        matches = []

        s1_range_start = len(s1)
        s1_range_end = 0
        s2_range_start = 0
        s2_range_end = 0

        i = len(s1)
        j = len(s2)
        while i > 0 and j > 0:
            emit_range = False
            if s1[i - 1] == s2[j - 1]:
                match_bytes = s1[i - 1 : i] + match_bytes

                if s1_range_start == len(s1):
                    s1_range_start = i - 1
                    s1_range_end = i - 1
                    s2_range_start = j - 1
                    s2_range_end = j - 1
                elif s1_range_start == i and s2_range_start == j:
                    s1_range_start -= 1
                    s2_range_start -= 1
                else:
                    emit_range = True

                if s1_range_start == 0 or s2_range_start == 0:
                    emit_range = True

                i -= 1
                j -= 1
            else:
                if matrix[i - 1][j] > matrix[i][j - 1]:
                    i -= 1
                else:
                    j -= 1
                if s1_range_start != len(s1):
                    emit_range = True

            if emit_range:
                match_length = s1_range_end - s1_range_start + 1
                if self.min_match_length == 0 or match_length >= self.min_match_length:
                    match: list = [
                        [s1_range_start, s1_range_end],
                        [s2_range_start, s2_range_end],
                    ]
                    if self.with_match_length:
                        match += [match_length]
                    matches.append(match)
                s1_range_start = len(s1)
        return matches, match_bytes

    def execute(self) -> ValueType:
        s1 = self.database.bytes_database.get_value_or_empty(self.key1)
        s2 = self.database.bytes_database.get_value_or_empty(self.key2)

        if self.length and self.index:
            raise ServerError(b"ERR If you want both the length and indexes, please just use IDX.")

        if len(s1) >= UINT32_MAX - 1 or len(s2) >= UINT32_MAX - 1:
            raise ServerError(b"ERR String too long for LCS")

        matrix = self.compute_matrix(s1, s2)
        lcs_length = self.get_lcs_length(matrix)

        if self.length:
            return lcs_length

        matches, match_bytes = self.get_lcs_matches(matrix, s1, s2)

        if not self.index:
            return match_bytes

        return {b"matches": matches, b"len": lcs_length}


@command(b"mget", {b"read", b"string"}, flags={b"fast", b"readonly"})
class MultipleGet(Command):
    """
    summary: Atomically returns the string values of one or more keys.
    complexity: O(N) where N is the number of keys to retrieve.
    since: 1.0.0
    function: mgetCommand
    reply_schema:
      description: List of values at the specified keys.
      type: array
      minItems: 1
      items:
        oneOf:
        - type: string
        - type: 'null'
    """

    database: Database = dependency()
    keys: list[bytes] = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        result: list[bytes | None] = []
        for key in self.keys:
            key_value = None
            try:
                key_value = self.database.bytes_database.get_or_none(key)
            except ServerWrongTypeError:
                pass

            if key_value is None:
                result.append(None)
            else:
                result.append(key_value.value)
        return result


class ExistenceMode(Enum):
    OnlyIfNotExist = b"NX"
    OnlyIfExist = b"XX"


@command(b"mset", {b"slow", b"string"}, flags={b"denyoom", b"write"})
class SetMultiple(Command):
    """
    summary: Atomically creates or modifies the string values of one or more keys.
    complexity: O(N) where N is the number of keys to set.
    since: 1.0.1
    function: msetCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, value in self.key_value:
            self.database.upsert(key, value)
        return RESP_OK


@command(
    b"msetex",
    {b"slow", b"string"},
    flags={b"denyoom", b"write"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class SetMultipleExpire(Command):
    """
    summary: >-
      Atomically creates or modifies the string values of one or more keys, and optionally set their expiration.
    complexity: O(N) where N is the number of keys to set.
    since: 9.1.0
    function: msetexCommand
    reply_schema:
      oneOf:
      - description: No key was set.
        const: 0
      - description: All the keys were set.
        const: 1
    """

    database: Database = dependency()
    numkeys: int = positional_parameter()
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW", length_field_name="numkeys")
    existence_mode: ExistenceMode | None = keyword_parameter(
        flag={b"NX": ExistenceMode.OnlyIfNotExist, b"XX": ExistenceMode.OnlyIfExist}, default=None
    )
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)
    keepttl: bool = flag_parameter(token=b"KEEPTTL")

    def execute(self) -> ValueType:
        ttl_flags = [self.ex, self.px, self.exat, self.pxat, self.keepttl]
        if sum(1 for f in ttl_flags if f) > 1:
            raise ServerError(b"ERR syntax error")

        expiration: int | None = None
        if self.ex is not None:
            expiration = now_ms() + self.ex * 1000
        elif self.px is not None:
            expiration = now_ms() + self.px
        elif self.exat is not None:
            expiration = self.exat * 1000
        elif self.pxat is not None:
            expiration = self.pxat

        if self.existence_mode == ExistenceMode.OnlyIfNotExist:
            for key, _ in self.key_value:
                if self.database.string_database.has_key(key):
                    return 0
        elif self.existence_mode == ExistenceMode.OnlyIfExist:
            for key, _ in self.key_value:
                if not self.database.string_database.has_key(key):
                    return 0

        for key, value in self.key_value:
            if self.keepttl:
                existing = self.database.string_database.get_or_none(key)
                current_expiration = existing.expiration if existing is not None else None
                self.database.string_database.set_key_value(
                    KeyValue.of_string(key, value, expiration=current_expiration)
                )
            else:
                self.database.string_database.set_key_value(KeyValue.of_string(key, value, expiration=expiration))
        return 1


@command(b"msetnx", {b"slow", b"string"}, flags={b"denyoom", b"write"})
class SetIfNotExistsMultiple(Command):
    """
    summary: >-
      Atomically modifies the string values of one or more keys only when all keys don't exist.
    complexity: O(N) where N is the number of keys to set.
    since: 1.0.1
    function: msetnxCommand
    reply_schema:
      oneOf:
      - description: No key was set (at least one key already existed).
        const: 0
      - description: All the keys were set.
        const: 1
    """

    database: Database = dependency()
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, _ in self.key_value:
            if self.database.string_database.has_key(key):
                return False
        for key, value in self.key_value:
            self.database.upsert(key, value)
        return True


@command(
    b"set",
    {b"slow", b"string"},
    flags={b"denyoom", b"write"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class Set(Command):
    """
    summary: >-
      Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: setCommand
    reply_schema:
      anyOf:
      - description: >-
          `GET` not given: Operation was aborted (conflict with one of the `XX`/`NX` options).
        type: 'null'
      - description: '`GET` not given: The key was set.'
        const: OK
      - description: '`GET` given: The key didn''t exist before the `SET`'
        type: 'null'
      - description: '`GET` given: The previous value of the key'
        type: string
    """

    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    existence_mode: ExistenceMode | None = keyword_parameter(
        flag={b"NX": ExistenceMode.OnlyIfNotExist, b"XX": ExistenceMode.OnlyIfExist}, default=None
    )
    condition: bytes | None = keyword_parameter(flag=b"IFEQ", default=None)
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)
    get: bool = flag_parameter(token=b"GET")

    _is_key_updated: bool = field(init=False, default=False)

    def get_one_and_only_token(self) -> str | None:
        fields_names = ["ex", "px", "exat", "pxat"]

        filled = list(map(bool, [getattr(self, name) for name in fields_names]))
        if filled.count(True) > 1:
            raise ServerError(b"ERR syntax error")

        if True in filled:
            name = fields_names[filled.index(True)]

            return name

        return None

    def execute(self) -> ValueType:
        expiration = None
        if token_name := self.get_one_and_only_token():
            token_value = getattr(self, token_name)
            if token_name in ["ex", "px"]:
                expiration = now_ms() + token_value * (1000 if token_name == "ex" else 1)
            if token_name in ["exat", "pxat"]:
                expiration = token_value * (1000 if token_name == "exat" else 1)

        if self.condition is not None and self.existence_mode is not None:
            raise ServerError(b"ERR syntax error")

        database: DatabaseBase = self.database
        if self.get or self.condition is not None:
            database = self.database.string_database

        previous_value = database.get_value_or_none(self.key)
        if previous_value is None:
            if self.existence_mode == ExistenceMode.OnlyIfExist:
                return None
        elif self.existence_mode == ExistenceMode.OnlyIfNotExist:
            return cast("ValueType", previous_value) if self.get else None

        if self.condition is not None and previous_value != self.condition:
            return None

        previous = self.database.pop(self.key, None)
        self.database.string_database.set_key_value(KeyValue.of_string(self.key, self.value, expiration=expiration))

        if previous is None:
            self.database.notify(NotificationType.NEW, b"new", self.key)

        self._is_key_updated = True

        self.database.notifications_manager.notify(NotificationType.STRING, b"set", self.key)

        return RESP_OK if not self.get else cast("ValueType", previous_value)

    async def after(self, in_multi: bool = False) -> None:
        if self._is_key_updated:
            await self.blocking_manager.notify_deleted(self.key, in_multi=in_multi)


@command(b"setex", {b"slow", b"string"}, flags={b"denyoom", b"write"})
class SetExpire(Command):
    """
    summary: >-
      Sets the string value and expiration time of a key. Creates the key if it doesn't exist.
    complexity: O(1)
    since: 2.0.0
    function: setexCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.string_database.upsert(self.key, self.value)
        self.database.set_expiration_in(self.key, 1000 * self.seconds)
        return RESP_OK


@command(b"psetex", {b"slow", b"string"}, flags={b"denyoom", b"write"})
class SetExpireMilliseconds(Command):
    """
    summary: >-
      Sets both string value and expiration time in milliseconds of a key. The key is created if it doesn't exist.
    complexity: O(1)
    since: 2.6.0
    function: psetexCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    milliseconds: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.string_database.upsert(self.key, self.value)
        self.database.set_expiration_in(self.key, self.milliseconds)
        return RESP_OK


@command(b"setnx", {b"string"}, flags={b"denyoom", b"fast", b"write"})
class SetIfNotExists(Command):
    """
    summary: Sets the string value of a key only when the key doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: setnxCommand
    reply_schema:
      oneOf:
      - description: The key was set.
        const: 0
      - description: The key was not set.
        const: 1
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")

    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.database.string_database.has_key(self.key):
            return False
        self.database.string_database.upsert(self.key, self.value)
        return True


@command(b"setrange", {b"slow", b"string"}, flags={b"denyoom", b"write"})
class SetRange(Command):
    """
    summary: >-
      Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist.
    complexity: >-
      O(1), not counting the time taken to copy the new string in place. Usually, this string is very small so the
      amortized complexity is O(1). Otherwise, complexity is O(M) with M being the length of the value argument.
    since: 2.2.0
    function: setrangeCommand
    reply_schema:
      description: Length of the string after it was modified by the command.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    offset: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.offset < 0:
            raise ServerError(b"ERR value is not an integer or out of range")

        if self.offset + len(self.value) > 512 * (1024**2):
            raise ServerError(b"ERR string exceeds maximum allowed size (proto-max-bulk-len)")

        string_value = self.database.bytes_database.get_value_or_empty(self.key)
        if not self.value:
            return len(string_value)

        if self.offset >= len(string_value):
            new_value = string_value + b"\x00" * (self.offset - len(string_value)) + self.value
        else:
            new_value = string_value[: self.offset] + self.value + string_value[self.offset + len(self.value) :]

        self.database.bytes_database.upsert(self.key, new_value)

        return len(new_value)


@command(b"strlen", {b"read", b"string"}, flags={b"fast", b"readonly"})
class StringLength(Command):
    """
    summary: Returns the length of a string value.
    complexity: O(1)
    since: 2.2.0
    function: strlenCommand
    reply_schema:
      description: The length of the string value stored at key, or 0 when key does not exist.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return len(self.database.bytes_database.get_value_or_empty(self.key))


@command(b"substr", {b"read", b"slow", b"string"}, flags={b"readonly"})
class StringSubstring(Command):
    """
    summary: Returns a substring from a string value.
    complexity: >-
      O(N) where N is the length of the returned string. The complexity is ultimately determined by the returned
      length, but because creating a substring from an existing string is very cheap, it can be considered O(1)
      for small strings.
    since: 1.0.0
    function: getrangeCommand
    reply_schema:
      type: string
      description: >-
        The substring of the string value stored at key, determined by the offsets start and end (both are inclusive).
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    start: int = positional_parameter()
    end: int = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.bytes_database.get_value_or_empty(self.key)
        return value[parse_range_parameters(self.start, self.end)]
