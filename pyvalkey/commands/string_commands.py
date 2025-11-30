from dataclasses import field
from enum import Enum
from math import isinf, isnan

from pyvalkey.commands.core import DatabaseCommand
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import increment_bytes_value_as_float, parse_range_parameters
from pyvalkey.consts import LONG_LONG_MIN, LONG_MAX, LONG_MIN, UINT32_MAX
from pyvalkey.database_objects.databases import Database, DatabaseBase, KeyValue, StreamBlockingManager
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.resp import RESP_OK, ValueType


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


@command(b"append", {b"write", b"string", b"fast"})
class Append(DatabaseCommand):
    key: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.bytes_database.get_value_or_empty(self.key)
        new_value = value + self.value
        self.database.bytes_database.upsert(self.key, new_value)
        return len(value)


@command(b"decr", {b"write", b"string", b"fast"})
class Decrement(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, -1)


@command(b"decrby", {b"write", b"string", b"fast"})
class DecrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    decrement: int = positional_parameter()

    def execute(self) -> ValueType:
        if self.decrement == LONG_LONG_MIN:
            raise ServerError(b"ERR decrement would overflow")

        return increment_by(self.database, self.key, self.decrement * -1)


@command(b"get", {b"read", b"string", b"fast"})
class Get(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return self.database.string_database.get_value_or_none(self.key)


@command(b"getdel", {b"read", b"string", b"fast"})
class GetDelete(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        key_value = self.database.string_database.pop(self.key, None)
        if key_value is not None:
            return key_value.value
        return None


@command(
    b"getex", {b"write", b"string", b"fast"}, metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"}
)
class GetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)
    persist: bool = keyword_parameter(flag=b"PERSIST")

    def execute(self) -> ValueType:
        print(self.ex, self.px, self.exat, self.pxat, self.persist)

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


@command(b"getrange", {b"stream", b"write", b"fast"})
class StringGetRange(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    end: int = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.bytes_database.get_value_or_empty(self.key)
        return value[parse_range_parameters(self.start, self.end)]


@command(b"getset", {b"write", b"string", b"slow"})
class GetSet(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        old_value = self.database.string_database.get_value_or_create(self.key)
        self.database.string_database.upsert(self.key, self.value)
        return old_value


@command(b"incr", {b"write", b"string", b"fast"})
class Increment(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key)


@command(b"incrby", {b"write", b"string", b"fast"})
class IncrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: int = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.increment)


@command(b"incrbyfloat", {b"write", b"string", b"fast"})
class IncrementByFloat(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: float = positional_parameter()

    def execute(self) -> ValueType:
        if isnan(self.increment) or isinf(self.increment):
            raise ServerError(b"ERR increment would produce NaN or Infinity")
        return increment_by(self.database, self.key, self.increment)


@command(b"lcs", {b"write", b"string", b"fast"})
class LongestCommonSubsequence(DatabaseCommand):
    key1: bytes = positional_parameter()
    key2: bytes = positional_parameter()
    length: bool = keyword_parameter(flag=b"LEN", default=False)
    index: bool = keyword_parameter(flag=b"IDX", default=False)
    min_match_length: int = keyword_parameter(token=b"MINMATCHLEN", default=0)
    with_match_length: bool = keyword_parameter(flag=b"WITHMATCHLEN", default=False)

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


@command(b"mget", {b"read", b"string", b"fast"})
class MultipleGet(DatabaseCommand):
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


@command(b"mset", {b"write", b"string", b"slow"})
class SetMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, value in self.key_value:
            self.database.upsert(key, value)
        return RESP_OK


@command(b"msetnx", {b"write", b"string", b"slow"})
class SetIfNotExistsMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, _ in self.key_value:
            if self.database.string_database.has_key(key):
                return False
        for key, value in self.key_value:
            self.database.upsert(key, value)
        return True


class ExistenceMode(Enum):
    OnlyIfNotExist = b"NX"
    OnlyIfExist = b"XX"


@command(b"set", {b"write", b"string", b"slow"})
class Set(DatabaseCommand):
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
    get: bool = keyword_parameter(flag=b"GET", default=False)

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
            return previous_value if self.get else None

        if self.condition is not None and previous_value != self.condition:
            return None

        self.database.pop(self.key, None)
        self.database.string_database.set_key_value(KeyValue.of_string(self.key, self.value, expiration=expiration))
        self._is_key_updated = True

        return RESP_OK if not self.get else previous_value

    async def after(self, in_multi: bool = False) -> None:
        if self._is_key_updated:
            await self.blocking_manager.notify_deleted(self.key, in_multi=in_multi)


@command(b"setex", {b"write", b"string", b"slow"})
class SetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.string_database.upsert(self.key, self.value)
        self.database.set_expiration_in(self.key, self.seconds)
        return RESP_OK


@command(b"setnx", {b"write", b"string", b"fast"})
class SetIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.database.string_database.has_key(self.key):
            return False
        self.database.string_database.upsert(self.key, self.value)
        return True


@command(b"setrange", {b"write", b"string", b"slow"})
class SetRange(DatabaseCommand):
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


@command(b"strlen", {b"read", b"string", b"fast"})
class StringLength(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return len(self.database.bytes_database.get_value_or_empty(self.key))


@command(b"substr", {b"stream", b"write", b"fast"})
class StringSubstring(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    end: int = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.bytes_database.get_value_or_empty(self.key)
        return value[parse_range_parameters(self.start, self.end)]
