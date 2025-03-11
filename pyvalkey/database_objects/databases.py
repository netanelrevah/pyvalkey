from __future__ import annotations

import functools
import itertools
import operator
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

from sortedcontainers import SortedDict, SortedSet

from pyvalkey.commands.consts import LFU_COUNTER_MAXIMUM, LFU_INITIAL_VALUE
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.utils import (
    is_integer,
)
from pyvalkey.database_objects.errors import ServerWrongTypeError
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


@dataclass(eq=True)
class RangeLimit:
    offset: int = positional_parameter()
    count: int = positional_parameter()


class ValkeySortedSet:
    def __init__(self, members_and_scores: Iterable[tuple[bytes, float]] | None = None) -> None:
        members_and_scores = members_and_scores or []
        self.members = SortedSet({(score, member) for member, score in members_and_scores})
        self.members_scores = SortedDict({member: score for member, score in members_and_scores})

    def update(self, *scored_members: tuple[float, bytes]) -> None:
        for score, member in scored_members:
            self.add(score, member)

    def update_with_iterator(self, iterator: Iterable[bytes | float]) -> None:
        score: float
        member: bytes
        for score, member in zip(*([iter(iterator)] * 2), strict=True):
            self.add(score, member)

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


KeyValueType = ValkeySortedSet | dict[bytes, bytes] | set[bytes] | list[bytes] | bytes | int


KeyValueTypeVar = TypeVar("KeyValueTypeVar", bound=KeyValueType)


@dataclass(slots=True)
class KeyValue(Generic[KeyValueTypeVar]):
    key: bytes
    value: KeyValueTypeVar
    expiration: int | None = field(default=None)
    last_accessed: int = field(default_factory=lambda: int(time.time() * 1000))
    lfu_counter: int = LFU_INITIAL_VALUE

    def increase_frequency(self, lfu_log_factor: int) -> None:
        if self.lfu_counter == LFU_COUNTER_MAXIMUM:
            return
        r = random.random()
        base_value = max(self.lfu_counter - LFU_INITIAL_VALUE, 0)
        p = 1.0 / (base_value * lfu_log_factor + 1)
        if r >= p:
            return
        self.lfu_counter += 1

    def __hash__(self) -> int:
        return hash(self.key)

    def copy(self, new_key: bytes) -> KeyValue:
        new_value: KeyValueTypeVar
        if isinstance(self.value, bytes | int):
            new_value = cast(KeyValueTypeVar, self.value)
        elif isinstance(self.value, dict):
            new_value = cast(KeyValueTypeVar, dict(self.value))
        elif isinstance(self.value, ValkeySortedSet):
            new_value = cast(KeyValueTypeVar, ValkeySortedSet(self.value.members_scores.items()))
        elif isinstance(self.value, list):
            new_value = cast(KeyValueTypeVar, list(self.value))
        elif isinstance(self.value, set):
            new_value = cast(KeyValueTypeVar, set(self.value))
        else:
            raise NotImplementedError(f"copy of {type(self.value)} not implemented")

        return KeyValue(new_key, new_value, self.expiration)

    @classmethod
    def of_string(cls, key: bytes, value: bytes, expiration: int | None = None) -> KeyValue[bytes] | KeyValue[int]:
        if is_integer(value):
            return KeyValue(key, int(value), expiration)
        return KeyValue(key, value, expiration)


MISSING: KeyValue = KeyValue(b"", {})


class DatabaseBase(Generic[KeyValueTypeVar]):
    data: dict[bytes, KeyValue[KeyValueTypeVar]]
    key_with_expiration: SortedSet

    def has_key_with_type_check(self, key: bytes, type_: type | None = None) -> bool:
        key_value = self.data.get(key, None)

        if not key_value:
            return False

        if key_value.expiration is not None:
            if int(time.time() * 1000) > key_value.expiration:
                self.data.pop(key)
                self.key_with_expiration.discard(key_value)
                return False

        if (
            type_ is not None
            and not isinstance(key_value.value, type_)
            and (
                (type_ is int and not isinstance(key_value.value, bytes))
                or (type_ is bytes and not isinstance(key_value.value, int))
            )
        ):
            raise ServerWrongTypeError()

        key_value.last_accessed = int(time.time() * 1000)

        return True

    def has_key(self, key: bytes) -> bool:
        raise NotImplementedError()

    def size(self) -> int:
        return len(self.data)

    def number_of_keys_with_expiration(self) -> int:
        return len(self.key_with_expiration)

    def average_ttl(self) -> int:
        return sum(key_value.expiration - int(time.time() * 1000) for key_value in self.key_with_expiration) // len(
            self.key_with_expiration
        )

    def keys(self) -> Iterable[bytes]:
        return self.data.keys()

    def are_fully_volatile(self) -> bool:
        return len(self.data) == len(self.key_with_expiration)

    def empty(self) -> bool:
        return not self.data

    def rename_unsafely(self, key: bytes, new_key: bytes) -> None:
        key_value = self.data.pop(key)
        if key_value.expiration is not None:
            self.key_with_expiration.discard(key_value)

        if new_key in self.data:
            new_key_value = self.data.pop(new_key)
            if new_key_value.expiration is not None:
                self.key_with_expiration.discard(new_key_value)

        key_value.key = new_key
        self.data[new_key] = key_value
        if key_value.expiration is not None:
            self.key_with_expiration.add(key_value)

    def copy_from(self, key_value: KeyValue, new_key: bytes) -> None:
        self.data[new_key] = key_value.copy(new_key)
        if key_value.expiration:
            self.key_with_expiration.add(key_value)

    def pop(self, key: bytes, default: KeyValue[KeyValueTypeVar] | None = MISSING) -> KeyValue[KeyValueTypeVar] | None:
        if not self.has_key(key):
            if default is MISSING:
                raise KeyError()
            return default

        key_value = self.data.pop(key)
        if key_value.expiration is not None:
            self.key_with_expiration.discard(key_value)
        return key_value

    def get(self, key: bytes) -> KeyValue[KeyValueTypeVar]:
        if not self.has_key(key):
            raise KeyError()
        return self.data[key]

    def convert_value_if_needed(self, value: KeyValueTypeVar) -> KeyValueTypeVar:
        return value

    def get_value(self, key: bytes) -> KeyValueTypeVar:
        return self.convert_value_if_needed(self.get(key).value)

    def get_or_none(self, key: bytes) -> KeyValue | None:
        if not self.has_key(key):
            return None
        return self.get(key)

    def create_empty(self) -> KeyValueTypeVar:
        raise NotImplementedError()

    def get_or_create(self, key: bytes) -> KeyValue:
        if not self.has_key(key):
            key_value = KeyValue(key, self.create_empty())
            self.set_key_value(key_value)
            return key_value
        return self.get(key)

    def get_value_or_create(self, key: bytes) -> KeyValueTypeVar:
        return self.get_or_create(key).value

    def set_key_value(self, key_value: KeyValue, block_overwrite: bool = False) -> None:
        if block_overwrite is True and key_value.key in self.data:
            raise KeyError()
        self.data[key_value.key] = key_value
        if key_value.expiration is not None:
            self.key_with_expiration.add(key_value)

    def set_value(self, key: bytes, value: KeyValueTypeVar) -> None:
        if not self.has_key(key):
            raise KeyError()
        self.data[key].value = value

    def upsert(self, key: bytes, value: KeyValueTypeVar) -> None:
        key_value = self.get_or_none(key)
        if key_value is None:
            self.set_key_value(KeyValue(key, value))
        else:
            key_value.value = value

    # volatile functions

    def set_persist(self, key: bytes) -> bool:
        try:
            key_value = self.get(key)
        except KeyError:
            return False

        if key_value.expiration is not None:
            self.key_with_expiration.remove(key_value)
        key_value.expiration = None
        return True

    def set_expiration(self, key: bytes, expiration_milliseconds: int) -> bool:
        try:
            key_value = self.get(key)
        except KeyError:
            return False

        key_value.expiration = int(time.time() * 1000) + expiration_milliseconds
        self.key_with_expiration.add(key_value)
        return True

    def set_expiration_at(self, key: bytes, expiration_milliseconds_at: int) -> bool:
        try:
            key_value = self.get(key)
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
        return key_value.expiration

    def get_time_to_live(self, key: bytes) -> int | None:
        key_value = self.get(key)
        if key_value is None:
            raise KeyError()
        if key_value.expiration is None:
            return None
        return key_value.expiration - int(time.time() * 1000)


@dataclass
class TypedDatabase(Generic[KeyValueTypeVar], DatabaseBase[KeyValueTypeVar]):
    data: dict[bytes, KeyValue[KeyValueTypeVar]]
    key_with_expiration: SortedSet
    type_: type[KeyValueTypeVar]
    empty_factory: Callable[[], KeyValueTypeVar]

    def has_key(self, key: bytes) -> bool:
        return super().has_key_with_type_check(key, self.type_)

    def create_empty(self) -> KeyValueTypeVar:
        return self.empty_factory()


@dataclass
class BytesDatabase(DatabaseBase[bytes]):
    data: dict[bytes, KeyValue[bytes]]
    key_with_expiration: SortedSet

    type_: type[bytes] = field(default=bytes, init=False)

    def create_empty(self) -> bytes:
        return b""

    def convert_value_if_needed(self, value: int | bytes) -> bytes:
        if isinstance(value, int):
            return str(value).encode()
        return value

    def has_key(self, key: bytes) -> bool:
        return self.has_key_with_type_check(key, bytes)


@dataclass
class Database(DatabaseBase[KeyValueType]):
    data: dict[bytes, KeyValue] = field(default_factory=dict)
    key_with_expiration: SortedSet = field(default_factory=create_empty_keys_with_expiration)

    string_database: TypedDatabase[bytes | int] = field(init=False)
    bytes_database: BytesDatabase = field(init=False)
    int_database: TypedDatabase[int] = field(init=False)
    sorted_set_database: TypedDatabase[ValkeySortedSet] = field(init=False)
    set_database: TypedDatabase[set] = field(init=False)
    hash_database: TypedDatabase[dict] = field(init=False)
    list_database: TypedDatabase[list] = field(init=False)

    def has_key(self, key: bytes) -> bool:
        return self.has_key_with_type_check(key)

    def __post_init__(self) -> None:
        self.string_database = TypedDatabase(self.data, self.key_with_expiration, bytes, lambda: b"")
        self.bytes_database = BytesDatabase(self.data, self.key_with_expiration)
        self.int_database = TypedDatabase(self.data, self.key_with_expiration, int, lambda: 0)
        self.sorted_set_database = TypedDatabase(self.data, self.key_with_expiration, ValkeySortedSet, ValkeySortedSet)
        self.set_database = TypedDatabase(self.data, self.key_with_expiration, set, set)
        self.hash_database = TypedDatabase(self.data, self.key_with_expiration, dict, dict)
        self.list_database = TypedDatabase(self.data, self.key_with_expiration, list, list)
