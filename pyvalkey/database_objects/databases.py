import functools
import itertools
from dataclasses import dataclass
from typing import Any, Iterable

from sortedcontainers import SortedDict, SortedSet

from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.database_objects.errors import ServerWrongTypeError
from pyvalkey.database_objects.utils import flatten


@functools.total_ordering
class MaxBytes(bytes):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, bytes):
            return False
        raise NotImplementedError()

    def __ge__(self, other: object) -> bool:
        if isinstance(other, bytes):
            return True
        raise NotImplementedError()


MAX_BYTES = MaxBytes()


@dataclass
class ServerString:
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

    def update_with_numeric_value(self, value: int | float) -> None:
        self.numeric_value = value
        self.bytes_value = f"{value:g}".encode()
        self.int_value = self.int_from_bytes(self.bytes_value)

    def update_with_int_value(self, value: int) -> None:
        self.int_value = value
        self.bytes_value = self.int_to_bytes(value)
        self.numeric_value = int(self.bytes_value) if self.bytes_value.isdigit() else None

    def update_with_bytes_value(self, value: bytes) -> None:
        self.int_value = self.int_from_bytes(value)
        self.bytes_value = value
        self.numeric_value = float(value) if self.is_float(value) else None

    def __len__(self) -> int:
        return len(self.bytes_value)


@dataclass(eq=True)
class RangeLimit:
    offset: int = positional_parameter()
    count: int = positional_parameter()


class ServerSortedSet:
    def __init__(self, members_and_scores: Iterable[tuple[bytes, float]] | None = None) -> None:
        members_and_scores = members_and_scores or []
        self.members = SortedSet({(score, member) for member, score in members_and_scores})
        self.members_scores = SortedDict({member: score for member, score in members_and_scores})

    def update(self, *scored_members: tuple[float, bytes]) -> None:
        for score, member in scored_members:
            self.add(score, member)

    def update_with_iterator(self, iterator: Iterable[bytes | float]) -> None:
        member = None
        for item in iterator:
            if member is None:
                member = item
            else:
                self.add(item, member)
                member = None

    def add(self, score: float, member: bytes) -> None:
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
        limit: RangeLimit | None = None,
    ) -> Iterable[bytes]:
        if not is_reversed:
            minimum = (min_score, (b"" if min_inclusive else MAX_BYTES))
            maximum = (max_score, (MAX_BYTES if max_inclusive else b""))
        else:
            min_inclusive, max_inclusive = max_inclusive, min_inclusive
            minimum = (max_score, (b"" if max_inclusive else MAX_BYTES))
            maximum = (min_score, (MAX_BYTES if min_inclusive else b""))

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
        with_scores: bool = False,
        is_reversed: bool = False,
        limit: RangeLimit | None = None,
    ) -> Iterable[bytes]:
        if not is_reversed:
            minimum, maximum = min_lex, max_lex
        else:
            minimum, maximum = max_lex, min_lex
            min_inclusive, max_inclusive = max_inclusive, min_inclusive

        iterator = self.members_scores.irange(minimum, maximum, (min_inclusive, max_inclusive), reverse=is_reversed)

        if limit:
            offset = limit.offset
            count = limit.count + limit.offset
        else:
            offset = None
            count = None
        for member in itertools.islice(iterator, offset, count):
            if with_scores:
                yield member
                yield self.members_scores[member]
            else:
                yield member

    def range(self, range_slice: slice, with_scores: bool = False) -> list[bytes]:
        result = self.members[range_slice]

        offset = None
        count = None
        if with_scores:
            return list(itertools.islice(flatten(result, reverse_sub_lists=True), offset, count))
        return list(itertools.islice((member for score, member in result), offset, count))

    def __len__(self) -> int:
        return len(self.members)


class Database(dict[bytes, Any]):
    def get_by_type(self, key: bytes, type_: type) -> Any:  # noqa: ANN401
        value = type_()
        if key in self:
            value = self[key]
        if not isinstance(value, type_):
            raise ServerWrongTypeError()
        return value

    def get_or_create_by_type(self, key: bytes, type_: type) -> Any:  # noqa: ANN401
        if key not in self:
            self[key] = type_()
        value = self[key]
        if not isinstance(value, type_):
            raise ServerWrongTypeError()
        return value

    def get_string(self, key: bytes) -> ServerString:
        return self.get_by_type(key, ServerString)

    def get_string_or_none(self, key: bytes) -> ServerString | None:
        if key not in self:
            return None
        s = self[key]
        if not isinstance(s, ServerString):
            raise ServerWrongTypeError()
        return s

    def get_or_create_string(self, key: bytes) -> ServerString:
        return self.get_or_create_by_type(key, ServerString)

    def get_hash_table(self, key: bytes) -> dict:
        return self.get_by_type(key, dict)

    def get_or_create_hash_table(self, key: bytes) -> dict:
        return self.get_or_create_by_type(key, dict)

    def get_list(self, key: bytes) -> list:
        return self.get_by_type(key, list)

    def get_or_create_list(self, key: bytes) -> list:
        return self.get_or_create_by_type(key, list)

    def get_sorted_set(self, key: bytes) -> ServerSortedSet:
        return self.get_by_type(key, ServerSortedSet)

    def get_or_create_sorted_set(self, key: bytes) -> ServerSortedSet:
        return self.get_or_create_by_type(key, ServerSortedSet)

    def get_set(self, key: bytes) -> set:
        return self.get_by_type(key, set)

    def get_or_create_set(self, key: bytes) -> set:
        return self.get_or_create_by_type(key, set)
