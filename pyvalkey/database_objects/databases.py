from __future__ import annotations

import functools
import itertools
import operator
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from sortedcontainers import SortedDict, SortedSet

from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.database_objects.utils import flatten


@functools.total_ordering
class MaxBytes(bytes):
    def less(self, other: Any) -> bool:  # noqa: ANN401
        if isinstance(other, bytes):
            return False
        raise NotImplementedError()

    def more(self, other: Any) -> bool:  # noqa: ANN401
        if isinstance(other, bytes):
            return True
        raise NotImplementedError()

    __eq__ = less
    __le__ = less
    __lt__ = less
    __ge__ = more
    __gt__ = more


MAX_BYTES = MaxBytes()


@dataclass(slots=True)
class KeyValue:
    key: bytes
    value: ServerSortedSet | dict | StringType | set
    expiration: int | None = field(default=None)

    def __hash__(self) -> int:
        return hash(self.key)


@dataclass
class StringType:
    value: bytes = b""

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

    @property
    def numeric_value(self) -> float:
        if not self.is_float(self.value):
            raise ServerError(b"ERR value is not an integer or out of range")
        return float(self.value)

    @numeric_value.setter
    def numeric_value(self, value: int | float) -> None:
        self.value = f"{value:g}".encode()

    @property
    def int_value(self) -> int:
        return self.int_from_bytes(self.value)

    @int_value.setter
    def int_value(self, value: int) -> None:
        self.value = self.int_to_bytes(value)

    def get_bit(self, offset: int) -> int:
        if offset >= 2**32 or offset < 0:
            raise ServerError(b"ERR bit offset is not an integer or out of range")

        bytes_offset = offset // 8
        byte_offset = offset - (bytes_offset * 8)

        adjusted_value = self.value
        if len(self.value) <= bytes_offset:
            adjusted_value = self.value.ljust(bytes_offset + 1, b"\0")

        return (adjusted_value[bytes_offset] >> (7 - byte_offset)) & 1

    def set_bit(self, offset: int, value: bool) -> None:
        if offset >= 2**32 or offset < 0:
            raise ServerError(b"ERR bit offset is not an integer or out of range")

        bytes_offset = offset // 8
        byte_offset = offset - (bytes_offset * 8)

        new_value = self.value

        if len(self.value) <= bytes_offset:
            new_value = self.value.ljust(bytes_offset + 1, b"\0")

        if value:
            new_byte = new_value[bytes_offset] | (128 >> byte_offset)
        else:
            new_byte = new_value[bytes_offset] & ~(128 >> byte_offset)

        self.value = new_value[:bytes_offset] + bytes([new_byte]) + new_value[bytes_offset + 1 :]

    def count_bits_of_bytes(self, start: int | None = None, stop: int | None = None) -> int:
        return sum(map(int.bit_count, self.value[slice(start, stop)]))

    def count_bits_of_int(self, start: int, stop: int) -> int:
        return ((self.int_value & ((2**stop) - 1)) >> start).bit_count()

    def bit_length(self) -> int:
        return self.int_value.bit_length()

    def __len__(self) -> int:
        return len(self.value)


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
    ) -> Iterable[bytes | float]:
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
    ) -> Iterable[bytes | float]:
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

    def range(self, range_slice: slice, with_scores: bool = False) -> list[bytes | float]:
        result = self.members[range_slice]

        offset = None
        count = None
        if with_scores:
            return list(itertools.islice(flatten(result, reverse_sub_lists=True), offset, count))
        return list(itertools.islice((member for score, member in result), offset, count))

    def __len__(self) -> int:
        return len(self.members)


def create_empty_keys_with_expiration() -> SortedSet:
    return SortedSet(key=operator.attrgetter("expiration"))


@dataclass
class Database:
    data: dict[bytes, KeyValue] = field(default_factory=dict)
    key_with_expiration: SortedSet = field(default_factory=create_empty_keys_with_expiration)

    def pop(self, key: bytes) -> KeyValue | None:
        key_value = self.data.pop(key, None)
        if key_value is None:
            return None
        if key_value.expiration is not None and int(time.time() * 1000) > key_value.expiration:
            self.key_with_expiration.discard(key_value)
            return None
        return key_value

    @classmethod
    def check_type_ang_get(cls, key_value: KeyValue | None, type_: type) -> KeyValue | None:
        if key_value is not None and not isinstance(key_value.value, type_):
            raise ServerWrongTypeError()
        return key_value

    def typesafe_pop(self, key: bytes, type_: type) -> KeyValue | None:
        key_value = self.pop(key)
        return self.check_type_ang_get(key_value, type_)

    def get(self, key: bytes) -> KeyValue | None:
        key_value = self.data[key]
        if key_value.expiration is not None and int(time.time() * 1000) > key_value.expiration:
            del self.data[key]
            self.key_with_expiration.remove(key_value)
            return None
        return key_value

    def typesafe_get(self, key: bytes, type_: type) -> KeyValue | None:
        key_value = self.get(key)
        return self.check_type_ang_get(key_value, type_)

    def unsafe_get(self, key: bytes) -> KeyValue:
        if key not in self.data:
            raise KeyError()
        key_value = self.data[key]
        if key_value.expiration is not None and int(time.time() * 1000) > key_value.expiration:
            del self.data[key]
            self.key_with_expiration.remove(key_value)
            raise KeyError()
        return key_value

    def set_persist(self, key: bytes) -> bool:
        try:
            key_value = self.unsafe_get(key)
        except KeyError:
            return False

        self.key_with_expiration.remove(key_value)
        key_value.expiration = None
        return True

    def set_expiration(self, key: bytes, expiration_milliseconds: int) -> bool:
        try:
            key_value = self.unsafe_get(key)
        except KeyError:
            return False

        key_value.expiration = int(time.time() * 1000) + expiration_milliseconds
        self.key_with_expiration.add(key_value)
        return True

    def set_expiration_at(self, key: bytes, expiration_milliseconds_at: int) -> bool:
        try:
            key_value = self.unsafe_get(key)
        except KeyError:
            return False

        key_value.expiration = expiration_milliseconds_at
        self.key_with_expiration.add(key_value)
        return True

    def get_expiration(self, key: bytes) -> int | None:
        key_value = self.get(key)
        if key_value is None:
            raise KeyError()
        if key_value.expiration is None:
            return None
        return key_value.expiration - int(time.time() * 1000)

    def get_by_type(self, key: bytes, type_: type) -> Any:  # noqa: ANN401
        key_value = None
        if key in self.data:
            key_value = self.typesafe_get(key, type_)

        return key_value.value if key_value else type_()

    def typesafe_get_or_create(self, key: bytes, type_: type) -> Any:  # noqa: ANN401
        key_value = None
        if key in self.data:
            key_value = self.typesafe_get(key, type_)

        if key_value is None:
            key_value = KeyValue(key, type_())
            self.data[key] = key_value

        return key_value.value

    def get_or_none_by_type(self, key: bytes, type_: type) -> Any:  # noqa: ANN401
        if key not in self.data:
            return None

        key_value = self.typesafe_get(key, type_)

        if key_value is None:
            return None

        return key_value.value

    def get_string(self, key: bytes) -> StringType:
        return self.get_by_type(key, StringType)

    def get_string_or_none(self, key: bytes) -> StringType | None:
        return self.get_or_none_by_type(key, StringType)

    def get_or_create_string(self, key: bytes) -> StringType:
        return self.typesafe_get_or_create(key, StringType)

    def get_hash_table(self, key: bytes) -> dict:
        return self.get_by_type(key, dict)

    def get_or_create_hash_table(self, key: bytes) -> dict:
        return self.typesafe_get_or_create(key, dict)

    def get_list(self, key: bytes) -> list:
        return self.get_by_type(key, list)

    def get_or_create_list(self, key: bytes) -> list:
        return self.typesafe_get_or_create(key, list)

    def get_sorted_set(self, key: bytes) -> ServerSortedSet:
        return self.get_by_type(key, ServerSortedSet)

    def get_or_create_sorted_set(self, key: bytes) -> ServerSortedSet:
        return self.typesafe_get_or_create(key, ServerSortedSet)

    def get_set(self, key: bytes) -> set:
        return self.get_by_type(key, set)

    def get_or_create_set(self, key: bytes) -> set:
        return self.typesafe_get_or_create(key, set)

    def pop_string(self, key: bytes) -> Any:  # noqa: ANN401
        key_value = self.typesafe_pop(key, StringType)
        if key_value is None:
            return None
        return key_value.value
