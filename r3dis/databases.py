import functools
import itertools
from dataclasses import dataclass
from typing import Any, Iterator

from sortedcontainers import SortedDict, SortedSet

from r3dis.errors import RedisWrongType
from r3dis.utils import flatten


@functools.total_ordering
class RedisMaxString:
    def __eq__(self, other):
        if isinstance(other, (bytes, str)):
            return False
        raise NotImplementedError()

    def __ge__(self, other):
        if isinstance(other, (bytes, str)):
            return True
        raise NotImplementedError()


MAX_STRING = RedisMaxString()


@dataclass
class RedisString:
    bytes_value: bytes = b""
    int_value: int = 0
    numeric_value: float | None = 0

    @classmethod
    def is_float(cls, value: bytes) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    @classmethod
    def int_to_bytes(cls, value: int) -> bytes:
        return value.to_bytes(length=(8 + (value + (value < 0)).bit_length()) // 8, byteorder="big", signed=True)

    @classmethod
    def int_from_bytes(cls, value: bytes) -> int:
        return int.from_bytes(value, byteorder="big", signed=True)

    def update_with_numeric_value(self, value: int | float):
        self.numeric_value = value
        self.bytes_value = f"{value:g}".encode()
        self.int_value = self.int_from_bytes(self.bytes_value)

    def update_with_int_value(self, value: int):
        self.int_value = value
        self.bytes_value = self.int_to_bytes(value)
        self.numeric_value = int(self.bytes_value) if self.bytes_value.isdigit() else None

    def update_with_bytes_value(self, value: bytes):
        self.int_value = self.int_from_bytes(value)
        self.bytes_value = value
        self.numeric_value = float(value) if self.is_float(value) else None

    def __len__(self):
        return len(self.bytes_value)


@dataclass
class RangeLimit:
    offset: int
    count: int


class RedisSortedSet:
    def __init__(self, members_and_scores: Iterator[tuple[bytes, float]] | None = None):
        members_and_scores = members_and_scores or []
        self.members = SortedSet({(score, member) for member, score in members_and_scores})
        self.members_scores = SortedDict({member: score for member, score in members_and_scores})

    def update(self, *scored_members: tuple[float, bytes]):
        for score, member in scored_members:
            self.add(score, member)

    def update_with_iterator(self, iterator):
        member = None
        for item in iterator:
            if member is None:
                member = item
            else:
                self.add(item, member)
                member = None

    def add(self, score: float, member: bytes):
        if member in self.members_scores:
            old_score = self.members_scores[member]
            self.members.remove((old_score, member))
        self.members_scores[member] = score
        self.members.add((score, member))

    def range_by_score(
        self,
        min_score: float,
        max_score: float,
        min_inclusive: bool,
        max_inclusive: bool,
        with_scores: bool = False,
        is_reversed: bool = False,
        limit: RangeLimit = None,
    ):
        if not is_reversed:
            minimum = (min_score, (b"" if min_inclusive else MAX_STRING))
            maximum = (max_score, (MAX_STRING if max_inclusive else b""))
        else:
            min_inclusive, max_inclusive = max_inclusive, min_inclusive
            minimum = (max_score, (b"" if max_inclusive else MAX_STRING))
            maximum = (min_score, (MAX_STRING if min_inclusive else b""))

        iterator = self.members.irange(minimum, maximum, (min_inclusive, max_inclusive), reverse=is_reversed)

        if limit:
            offset = limit.offset
            count = limit.count + limit.offset
        else:
            offset = None
            count = None
        for score, member in itertools.islice(iterator, offset, count):
            if with_scores:
                yield member
                yield score
            else:
                yield member

    def range_by_lexical(
        self,
        min_lex: bytes,
        max_lex: bytes,
        min_inclusive: bool,
        max_inclusive: bool,
        with_scores=False,
        is_reversed=False,
        limit=None,
    ):
        if not is_reversed:
            minimum, maximum = min_lex, max_lex
        else:
            minimum, maximum = max_lex, min_lex
            min_inclusive, max_inclusive = max_inclusive, min_inclusive

        iterator = self.members_scores.irange(minimum, maximum, (min_inclusive, max_inclusive), reverse=is_reversed)

        if limit:
            offset = limit["offset"]
            count = limit["count"] + limit["offset"]
        else:
            offset = None
            count = None
        for member in itertools.islice(iterator, offset, count):
            if with_scores:
                yield member
                yield self.members_scores[member]
            else:
                yield member

    def range(self, range_slice, with_scores=False, limit=None):
        result = self.members[range_slice]

        if limit:
            offset = limit["offset"]
            count = limit["count"] + limit["offset"]
        else:
            offset = None
            count = None
        if with_scores:
            return list(itertools.islice(flatten(result, reverse_sub_lists=True), offset, count))
        return list(itertools.islice((member for score, member in result), offset, count))

    def __len__(self):
        return len(self.members)


class Database(dict[bytes, Any]):
    def get_by_type(self, key, type_):
        value = type_()
        if key in self:
            value = self[key]
        if not isinstance(value, type_):
            raise RedisWrongType()
        return value

    def get_or_create_by_type(self, key, type_):
        if key not in self:
            self[key] = type_()
        value = self[key]
        if not isinstance(value, type_):
            raise RedisWrongType()
        return value

    def get_string(self, key) -> RedisString:
        return self.get_by_type(key, RedisString)

    def get_string_or_none(self, key) -> RedisString | None:
        if key not in self:
            return None
        s = self[key]
        if not isinstance(s, RedisString):
            raise RedisWrongType()
        return s

    def get_or_create_string(self, key) -> RedisString:
        return self.get_or_create_by_type(key, RedisString)

    def get_hash_table(self, key) -> dict:
        return self.get_by_type(key, dict)

    def get_or_create_hash_table(self, key) -> dict:
        return self.get_or_create_by_type(key, dict)

    def get_list(self, key) -> list:
        return self.get_by_type(key, list)

    def get_or_create_list(self, key) -> list:
        return self.get_or_create_by_type(key, list)

    def get_sorted_set(self, key) -> RedisSortedSet:
        return self.get_by_type(key, RedisSortedSet)

    def get_or_create_sorted_set(self, key) -> RedisSortedSet:
        return self.get_or_create_by_type(key, RedisSortedSet)

    def get_set(self, key) -> set:
        return self.get_by_type(key, set)

    def get_or_create_set(self, key) -> set:
        return self.get_or_create_by_type(key, set)
