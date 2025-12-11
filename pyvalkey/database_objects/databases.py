from __future__ import annotations

import operator
import random
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from sortedcontainers import SortedSet

from pyvalkey.commands.utils import is_integer
from pyvalkey.consts import LFU_COUNTER_MAXIMUM, LFU_INITIAL_VALUE
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.database_objects.scored_sorted_set import ScoredSortedSet
from pyvalkey.database_objects.stream import Stream
from pyvalkey.enums import NotificationType
from pyvalkey.notifications import NotificationsManager
from pyvalkey.utils.times import now_ms

if TYPE_CHECKING:
    pass


def create_empty_keys_with_expiration() -> SortedSet:
    return SortedSet(key=operator.attrgetter("expiration"))


KeyValueType = ScoredSortedSet | dict[bytes, bytes | int] | set[bytes] | list[bytes] | bytes | int | Stream


KeyValueTypeVar = TypeVar("KeyValueTypeVar", bound=KeyValueType)


@dataclass(slots=True)
class KeyValue(Generic[KeyValueTypeVar]):
    key: bytes
    value: KeyValueTypeVar
    expiration: int | None = field(default=None)
    last_accessed: int = field(default_factory=lambda: now_ms())
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
        elif isinstance(self.value, ScoredSortedSet):
            new_value = cast(KeyValueTypeVar, ScoredSortedSet(self.value.members_scores.items()))
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


@dataclass
class DatabaseContent:
    data: dict[bytes, KeyValue] = field(default_factory=dict)
    key_with_expiration: SortedSet = field(default_factory=create_empty_keys_with_expiration)
    watchlist: dict[bytes, set[ClientWatchlist]] = field(default_factory=dict)

    def clear_key(self, key_value: KeyValue) -> None:
        if key_value.expiration is not None:
            self.key_with_expiration.discard(key_value)
        self.data.pop(key_value.key)

    def size(self) -> int:
        return len(self.data)

    def number_of_keys_with_expiration(self) -> int:
        return len(self.key_with_expiration)

    def average_ttl(self) -> int:
        if not self.key_with_expiration:
            return 0

        return sum(key_value.expiration - now_ms() for key_value in self.key_with_expiration) // len(
            self.key_with_expiration
        )

    def are_fully_volatile(self) -> bool:
        return len(self.data) == len(self.key_with_expiration)


class DatabaseBase(Generic[KeyValueTypeVar]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    def is_empty(self, value: KeyValueTypeVar) -> bool:
        if isinstance(value, bytes | int | list | set | dict):
            return not value
        if isinstance(value, ScoredSortedSet):
            return len(value) == 0
        if isinstance(value, Stream):
            return False
        raise TypeError()

    def has_typed_key(self, key: bytes, type_check: Callable[[KeyValueTypeVar], bool] | None = None) -> bool:
        key_value = self.content.data.get(key, None)

        if not key_value:
            return False

        if key_value.expiration is not None:
            if now_ms() > key_value.expiration:
                self.content.clear_key(key_value)
                self.notify(NotificationType.EXPIRED, b"expired", key)
                return False

        if self.is_empty(key_value.value):
            self.content.clear_key(key_value)
            return False

        if type_check is not None and not type_check(key_value.value):
            raise ServerWrongTypeError()

        key_value.last_accessed = now_ms()

        return True

    def has_key(self, key: bytes) -> bool:
        raise NotImplementedError()

    def size(self) -> int:
        return self.content.size()

    def number_of_keys_with_expiration(self) -> int:
        return self.content.number_of_keys_with_expiration()

    def average_ttl(self) -> int:
        return self.content.average_ttl()

    def keys(self) -> Iterable[bytes]:
        return self.content.data.keys()

    def are_fully_volatile(self) -> bool:
        return self.content.are_fully_volatile()

    def empty(self) -> bool:
        return not self.content

    def rename_unsafely(self, key: bytes, new_key: bytes) -> None:
        key_value = self.content.data.pop(key)
        if key_value.expiration is not None:
            self.content.key_with_expiration.discard(key_value)

        if new_key in self.content.data:
            new_key_value = self.content.data.pop(new_key)
            if new_key_value.expiration is not None:
                self.content.key_with_expiration.discard(new_key_value)

        key_value.key = new_key

        self.set_key_value(key_value)

    def copy_from(self, key_value: KeyValue, new_key: bytes) -> None:
        self.set_key_value(key_value.copy(new_key))

    def touch_all_database_watched_keys(self) -> None:
        for key in self.content.data.keys():
            self.touch_watched_key(key)

    def touch_watched_key(self, key: bytes) -> None:
        for client_watchlist in self.content.watchlist.get(key, []):
            print(f"key '{key.decode()}' in database {self.index} has been touched")
            client_watchlist.watchlist[(self.index, key)] = True

    def add_key_to_watchlist(self, key: bytes, client_watchlist: ClientWatchlist) -> None:
        if key not in self.content.watchlist:
            self.content.watchlist[key] = set()
        self.content.watchlist[key].add(client_watchlist)
        client_watchlist.watchlist[(self.index, key)] = False

    def pop_unsafely(self, key: bytes) -> KeyValue[KeyValueTypeVar]:
        key_value = self.content.data.pop(key)
        if key_value.expiration is not None:
            self.content.key_with_expiration.discard(key_value)
        self.touch_watched_key(key)
        return key_value

    def pop(self, key: bytes, default: KeyValue[KeyValueTypeVar] | None = MISSING) -> KeyValue[KeyValueTypeVar] | None:
        if not self.has_key(key):
            if default is MISSING:
                raise KeyError()
            return default

        return self.pop_unsafely(key)

    def get(self, key: bytes) -> KeyValue[KeyValueTypeVar]:
        if not self.has_key(key):
            raise KeyError()
        return self.content.data[key]

    @contextmanager
    def get_with_context(self, key: bytes) -> Iterator[KeyValue[KeyValueTypeVar]]:
        key_value = self.get(key)
        yield key_value
        if self.is_empty(key_value.value):
            self.pop_unsafely(key)

    def get_or_none(self, key: bytes) -> KeyValue[KeyValueTypeVar] | None:
        if not self.has_key(key):
            return None
        return self.content.data[key]

    def create_empty(self) -> KeyValueTypeVar:
        raise NotImplementedError()

    def get_or_create(self, key: bytes) -> KeyValue[KeyValueTypeVar]:
        if not self.has_key(key):
            key_value = KeyValue(key, self.create_empty())
            self.set_key_value(key_value)
            return key_value
        return self.content.data[key]

    @contextmanager
    def get_or_create_with_context(self, key: bytes) -> Iterator[KeyValue[KeyValueTypeVar]]:
        key_value = self.get_or_create(key)
        yield key_value
        if self.is_empty(key_value.value):
            self.pop_unsafely(key)

    def convert_value_if_needed(self, value: KeyValueTypeVar) -> KeyValueTypeVar:
        return value

    def get_value(self, key: bytes) -> KeyValueTypeVar:
        return self.convert_value_if_needed(self.get(key).value)

    @contextmanager
    def get_value_with_context(self, key: bytes) -> Iterator[KeyValueTypeVar]:
        value = self.get_value(key)
        yield value
        if self.is_empty(value):
            self.pop_unsafely(key)

    def get_value_or_empty(self, key: bytes) -> KeyValueTypeVar:
        if not self.has_key(key):
            return self.create_empty()
        return self.convert_value_if_needed(self.get(key).value)

    def get_value_or_none(self, key: bytes) -> KeyValueTypeVar | None:
        if not self.has_key(key):
            return None
        return self.convert_value_if_needed(self.get(key).value)

    def get_value_or_create(self, key: bytes) -> KeyValueTypeVar:
        return self.convert_value_if_needed(self.get_or_create(key).value)

    def set_key_value(self, key_value: KeyValue, *, block_overwrite: bool = False) -> None:
        if block_overwrite is True and key_value.key in self.content.data:
            raise KeyError()
        self.content.data[key_value.key] = key_value
        if key_value.expiration is not None:
            self.content.key_with_expiration.add(key_value)
        self.touch_watched_key(key_value.key)

    def set_value(self, key: bytes, value: KeyValueTypeVar) -> None:
        if not self.has_key(key):
            raise KeyError()
        self.content.data[key].value = value

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
            self.content.key_with_expiration.remove(key_value)
        key_value.expiration = None
        return True

    def set_expiration_in(self, key: bytes, expiration_milliseconds: int) -> bool:
        try:
            key_value = self.get(key)
        except KeyError:
            return False

        key_value.expiration = now_ms() + expiration_milliseconds
        self.content.key_with_expiration.add(key_value)
        return True

    def set_expiration_at(self, key: bytes, expiration_milliseconds_at: int) -> bool:
        try:
            key_value = self.get(key)
        except KeyError:
            return False

        key_value.expiration = expiration_milliseconds_at
        self.content.key_with_expiration.add(key_value)
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
        return key_value.expiration - now_ms()

    def notify(self, notification_type: NotificationType, event: bytes, key: bytes) -> None:
        self.notifications_manager.notify(notification_type, event, key)


@dataclass
class TypedDatabase(Generic[KeyValueTypeVar], DatabaseBase[KeyValueTypeVar]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    type_: type[KeyValueTypeVar]
    empty_factory: Callable[[], KeyValueTypeVar]
    emptiness_predicate: Callable[[KeyValueTypeVar], bool]

    def is_empty(self, value: KeyValueTypeVar) -> bool:
        return self.emptiness_predicate(value)

    def has_key(self, key: bytes) -> bool:
        return super().has_typed_key(key, lambda value: isinstance(value, self.type_))

    def create_empty(self) -> KeyValueTypeVar:
        return self.empty_factory()


@dataclass
class ListDatabase(DatabaseBase[list]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    def is_empty(self, value: list) -> bool:
        return not value

    def create_empty(self) -> list:
        return []

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key, lambda value: isinstance(value, list))


@dataclass
class BytesDatabase(DatabaseBase[bytes]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    def is_empty(self, value: int | bytes) -> bool:
        return value == b""

    def create_empty(self) -> bytes:
        return b""

    def convert_value_if_needed(self, value: int | bytes) -> bytes:
        if isinstance(value, int):
            return str(value).encode()
        return value

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key, lambda value: isinstance(value, bytes | int))


@dataclass
class IntDatabase(DatabaseBase[int]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    def is_empty(self, value: int | bytes) -> bool:
        return value == b""

    def create_empty(self) -> int:
        return 0

    def convert_value_if_needed(self, value: int | bytes) -> int:
        if isinstance(value, int):
            return value
        if not is_integer(value):
            raise ServerError(b"ERR value is not an integer or out of range")
        return int(value)

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key, lambda value: isinstance(value, int) or (isinstance(value, bytes)))


@dataclass
class StringDatabase(DatabaseBase[bytes | int]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    def is_empty(self, value: int | bytes) -> bool:
        return value == b""

    def create_empty(self) -> bytes:
        return b""

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key, lambda value: isinstance(value, bytes | int))


@dataclass
class AnySetDatabase(DatabaseBase[ScoredSortedSet | set[bytes]]):
    index: int
    content: DatabaseContent
    configurations: Configurations
    notifications_manager: NotificationsManager

    def is_empty(self, value: ScoredSortedSet | set[bytes]) -> bool:
        return len(value) == 0

    def create_empty(self) -> ScoredSortedSet | set[bytes]:
        return ScoredSortedSet()

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key, lambda value: isinstance(value, ScoredSortedSet | set))


@dataclass
class Database(DatabaseBase[KeyValueType]):
    index: int
    configurations: Configurations
    notifications_manager: NotificationsManager

    content: DatabaseContent = field(default_factory=DatabaseContent)

    string_database: StringDatabase = field(init=False)
    bytes_database: BytesDatabase = field(init=False)
    int_database: IntDatabase = field(init=False)
    sorted_set_database: TypedDatabase[ScoredSortedSet] = field(init=False)
    set_database: TypedDatabase[set] = field(init=False)
    hash_database: TypedDatabase[dict[bytes, int | bytes]] = field(init=False)
    list_database: ListDatabase = field(init=False)
    stream_database: TypedDatabase[Stream] = field(init=False)

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key)

    def __post_init__(self) -> None:
        self.string_database = StringDatabase(self.index, self.content, self.configurations, self.notifications_manager)
        self.bytes_database = BytesDatabase(self.index, self.content, self.configurations, self.notifications_manager)
        self.int_database = IntDatabase(self.index, self.content, self.configurations, self.notifications_manager)
        self.sorted_set_database = TypedDatabase(
            self.index,
            self.content,
            self.configurations,
            self.notifications_manager,
            ScoredSortedSet,
            ScoredSortedSet,
            lambda value: len(value) == 0,
        )
        self.any_set_database = AnySetDatabase(
            self.index, self.content, self.configurations, self.notifications_manager
        )
        self.set_database = TypedDatabase(
            self.index, self.content, self.configurations, self.notifications_manager, set, set, lambda value: not value
        )
        self.hash_database = TypedDatabase(
            self.index,
            self.content,
            self.configurations,
            self.notifications_manager,
            dict,
            dict,
            lambda value: not value,
        )
        self.list_database = ListDatabase(self.index, self.content, self.configurations, self.notifications_manager)

        self.stream_database = TypedDatabase(
            self.index,
            self.content,
            self.configurations,
            self.notifications_manager,
            Stream,
            Stream,
            lambda value: False,
        )

    def replace_content(self, new_content: DatabaseContent) -> None:
        for database in (
            self,
            self.string_database,
            self.bytes_database,
            self.int_database,
            self.sorted_set_database,
            self.set_database,
            self.hash_database,
            self.list_database,
            self.any_set_database,
            self.stream_database,
        ):
            database.content = new_content


@dataclass(unsafe_hash=True)
class ClientWatchlist:
    watchlist: dict[tuple[int, bytes], bool] = field(default_factory=dict, hash=False)
