from __future__ import annotations

import operator
import random
import time
from asyncio import Queue, wait_for
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from sortedcontainers import SortedSet

from pyvalkey.commands.utils import _decrease_entry_id, _format_entry_id, _parse_strict_entry_id, is_integer
from pyvalkey.consts import LFU_COUNTER_MAXIMUM, LFU_INITIAL_VALUE
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.database_objects.scored_sorted_set import ScoredSortedSet
from pyvalkey.database_objects.stream import Consumer, ConsumerGroup, EntryID, Stream
from pyvalkey.enums import StreamSpecialIds, UnblockMessage
from pyvalkey.utils.collections import SetMapping

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext


def create_empty_keys_with_expiration() -> SortedSet:
    return SortedSet(key=operator.attrgetter("expiration"))


KeyValueType = ScoredSortedSet | dict[bytes, bytes | int] | set[bytes] | list[bytes] | bytes | int | Stream


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

        return sum(key_value.expiration - int(time.time() * 1000) for key_value in self.key_with_expiration) // len(
            self.key_with_expiration
        )

    def are_fully_volatile(self) -> bool:
        return len(self.data) == len(self.key_with_expiration)


class DatabaseBase(Generic[KeyValueTypeVar]):
    index: int
    content: DatabaseContent

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
            if int(time.time() * 1000) > key_value.expiration:
                self.content.clear_key(key_value)
                return False

        if self.is_empty(key_value.value):
            self.content.clear_key(key_value)
            return False

        if type_check is not None and not type_check(key_value.value):
            raise ServerWrongTypeError()

        key_value.last_accessed = int(time.time() * 1000)

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

        key_value.expiration = int(time.time() * 1000) + expiration_milliseconds
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
        return key_value.expiration - int(time.time() * 1000)


@dataclass
class TypedDatabase(Generic[KeyValueTypeVar], DatabaseBase[KeyValueTypeVar]):
    index: int
    content: DatabaseContent

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

    def is_empty(self, value: ScoredSortedSet | set[bytes]) -> bool:
        return len(value) == 0

    def create_empty(self) -> ScoredSortedSet | set[bytes]:
        return ScoredSortedSet()

    def has_key(self, key: bytes) -> bool:
        return self.has_typed_key(key, lambda value: isinstance(value, ScoredSortedSet | set))


@dataclass
class Database(DatabaseBase[KeyValueType]):
    index: int
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
        self.string_database = StringDatabase(self.index, self.content)
        self.bytes_database = BytesDatabase(self.index, self.content)
        self.int_database = IntDatabase(self.index, self.content)
        self.sorted_set_database = TypedDatabase(
            self.index, self.content, ScoredSortedSet, ScoredSortedSet, lambda value: len(value) == 0
        )
        self.any_set_database = AnySetDatabase(self.index, self.content)
        self.set_database = TypedDatabase(self.index, self.content, set, set, lambda value: not value)
        self.hash_database = TypedDatabase(self.index, self.content, dict, dict, lambda value: not value)
        self.list_database = ListDatabase(self.index, self.content)

        self.stream_database = TypedDatabase(self.index, self.content, Stream, Stream, lambda value: False)

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


@dataclass
class BlockingManagerBase:
    notifications: SetMapping[bytes, Queue] = field(default_factory=SetMapping)
    lazy_notification_keys: list[bytes] = field(default_factory=list)

    def has_key(self, database: Database, key: bytes) -> bool:
        raise NotImplementedError()

    def is_instance(self, value: KeyValueTypeVar) -> bool:
        raise NotImplementedError()

    async def wait_for_lists(
        self,
        client_context: ClientContext,
        keys: list[bytes],
        timeout: int | float | None = None,
        *,
        in_multi: bool = False,
        clear_queue: bool = True,
    ) -> bytes | None:
        if timeout is not None and timeout < 0:
            raise ServerError(b"ERR timeout is negative")

        for key in keys:
            if not self.has_key(client_context.database, key):
                continue
            return key

        if in_multi:
            return None

        client_context.current_client.blocking_queue = Queue()
        self.notifications.add_multiple(keys, client_context.current_client.blocking_queue)

        try:
            while True:
                print(f"{client_context.current_client.client_id} waiting queue for keys {keys}")
                key = await wait_for(client_context.current_client.blocking_queue.get(), timeout=timeout or None)
                if key == UnblockMessage.ERROR:
                    print(f"{client_context.current_client.client_id} got unblock error from queue")
                    raise ServerError(b"UNBLOCKED client unblocked via CLIENT UNBLOCK")
                if key == UnblockMessage.TIMEOUT:
                    print(f"{client_context.current_client.client_id} got unblock timeout from queue")
                    raise TimeoutError()
                print(f"{client_context.current_client.client_id} got '{key.decode()}' from queue")
                if self.has_key(client_context.database, key):
                    return key
                print(f"{client_context.current_client.client_id} key '{key.decode()}' not in database, continue...")
        except TimeoutError:
            return None
        finally:
            self.notifications.remove_all(client_context.current_client.blocking_queue)
            if clear_queue:
                client_context.current_client.blocking_queue = None

    async def notify(self, key: bytes, in_multi: bool = False) -> None:
        if in_multi:
            print(f"adding '{key.decode()}' to lazy notification keys")
            self.lazy_notification_keys.append(key)
            return
        for queue in self.notifications.iter_values(key):
            print(f"putting '{key.decode()}' into queue")
            await queue.put(key)

    async def notify_safely(self, database: Database, key: bytes, in_multi: bool = False) -> None:
        try:
            if self.has_key(database, key):
                await self.notify(key, in_multi=in_multi)
        except ServerWrongTypeError:
            pass

    async def notify_lazy(self, database: Database) -> None:
        while self.lazy_notification_keys:
            key = self.lazy_notification_keys.pop(0)
            if key not in database.content.data or not self.is_instance(database.content.data[key].value):
                print(f"lazy key '{key.decode()}' not found, continue...")
                continue
            for queue in self.notifications.iter_values(key):
                print(f"putting '{key.decode()}' into queue")
                await queue.put(key)


class ListBlockingManager(BlockingManagerBase):
    def has_key(self, database: Database, key: bytes) -> bool:
        return database.list_database.has_key(key)

    def is_instance(self, value: KeyValueTypeVar) -> bool:
        return isinstance(value, list)


class SortedSetBlockingManager(BlockingManagerBase):
    def has_key(self, database: Database, key: bytes) -> bool:
        return database.sorted_set_database.has_key(key)

    def is_instance(self, value: KeyValueTypeVar) -> bool:
        return isinstance(value, ScoredSortedSet)


@dataclass
class LazyNotification:
    key: bytes
    deleted: bool
    created_in_transaction: bool


@dataclass
class StreamWaitingContext:
    keys_to_minimum_id: dict[bytes, EntryID] = field(default_factory=dict)
    key_to_consumer_group: dict[bytes, ConsumerGroup] = field(default_factory=dict)
    key_to_consumer: dict[bytes, Consumer] = field(default_factory=dict)
    key_to_history_only: dict[bytes, bool] = field(default_factory=dict)

    def reset(self) -> None:
        self.keys_to_minimum_id.clear()
        self.key_to_consumer_group.clear()
        self.key_to_consumer.clear()
        self.key_to_history_only.clear()


@dataclass
class StreamBlockingManager:
    notifications: SetMapping[bytes, Queue[bytes | UnblockMessage]] = field(default_factory=SetMapping)
    lazy_notification_keys: OrderedDict[bytes, LazyNotification] = field(default_factory=OrderedDict)

    def has_key(self, database: Database, key: bytes) -> bool:
        return database.stream_database.has_key(key)

    def is_instance(self, value: KeyValueTypeVar) -> bool:
        return isinstance(value, Stream)

    def calculate_group_context(
        self,
        database: Database,
        group_name: bytes,
        consumer_name: bytes,
        keys_to_ids: dict[bytes, bytes],
        waiting_context: StreamWaitingContext,
    ) -> bool:
        for key, id_ in keys_to_ids.items():
            value = database.stream_database.get_value_or_empty(key)
            if value is None or (value is not None and value.consumer_groups.get(group_name) is None):
                raise ServerError(
                    f"NOGROUP No such key '{key.decode()}' or consumer group '{group_name.decode()}'"
                    f" in XREADGROUP with GROUP option".encode()
                )
            waiting_context.key_to_consumer_group[key] = value.consumer_groups[group_name]

            group = value.consumer_groups[group_name]
            consumer = group.consumers.get(consumer_name, None)
            if consumer is None:
                consumer = Consumer(consumer_name)
                group.consumers[consumer_name] = consumer
            waiting_context.key_to_consumer[key] = consumer

            if id_ == StreamSpecialIds.NEW_GROUP_ENTRY_ID:
                waiting_context.keys_to_minimum_id[key] = group.last_id
                waiting_context.key_to_history_only[key] = False
            else:
                waiting_context.key_to_history_only[key] = True
                try:
                    waiting_context.keys_to_minimum_id[key] = _parse_strict_entry_id(id_, sequence_fill=0)
                except ValueError:
                    raise ServerError(b"ERR wrong type of argument for 'xread' command")

            if (
                id_ != StreamSpecialIds.NEW_GROUP_ENTRY_ID
                or value.after(waiting_context.keys_to_minimum_id[key]) is not None
            ):
                return True
        return False

    async def wait_for_group(
        self,
        client_context: ClientContext,
        group_name: bytes,
        consumer_name: bytes,
        keys_to_ids: dict[bytes, bytes],
        block_milliseconds: int | float | None = None,
        in_multi: bool = False,
        waiting_context: StreamWaitingContext | None = None,
    ) -> dict[bytes, EntryID] | None:
        print(
            f"waiting for group {group_name.decode()} and consumer {consumer_name.decode()} "
            f"with streams {keys_to_ids.keys()} for {block_milliseconds} ms"
        )

        waiting_context = waiting_context if waiting_context is not None else StreamWaitingContext()

        if self.calculate_group_context(
            client_context.database, group_name, consumer_name, keys_to_ids, waiting_context
        ):
            return waiting_context.keys_to_minimum_id

        if in_multi or block_milliseconds is None:
            return None

        client_context.current_client.blocking_queue = Queue()
        self.notifications.add_multiple(keys_to_ids.keys(), client_context.current_client.blocking_queue)

        deadline = (time.time_ns() + (block_milliseconds * 1_000_000)) if block_milliseconds != 0 else None
        try:
            while True:
                start_time = time.time_ns()
                if deadline is not None and start_time >= deadline:
                    break

                print(f"{client_context.current_client} time is {start_time}, deadline is {deadline}")

                timeout = ((deadline - start_time) / 1_000_000_000) if deadline is not None else None

                print(
                    f"{client_context.current_client.client_id} waiting queue "
                    f"for keys {keys_to_ids.keys()} with timeout {timeout}"
                )

                try:
                    queue_item = await wait_for(client_context.current_client.blocking_queue.get(), timeout=timeout)

                    if queue_item == UnblockMessage.ERROR:
                        print(f"{client_context.current_client.client_id} got unblock error from queue")
                        raise ServerError(b"UNBLOCKED client unblocked via CLIENT UNBLOCK")
                    if queue_item == UnblockMessage.TIMEOUT:
                        print(f"{client_context.current_client.client_id} got unblock timeout from queue")
                        raise TimeoutError()

                    key = queue_item

                    if key not in waiting_context.keys_to_minimum_id:
                        raise Exception()

                    print(f"got queue item for grouped stream '{key.decode()}', recalculating context")
                    waiting_context.reset()
                    if self.calculate_group_context(
                        client_context.database, group_name, consumer_name, keys_to_ids, waiting_context
                    ):
                        return waiting_context.keys_to_minimum_id
                except TimeoutError:
                    if time.time_ns() == start_time:
                        print("got timeout, sleeping for 1 ms")
                        time.sleep(0.001)
                    continue
        finally:
            self.notifications.remove_all(client_context.current_client.blocking_queue)
            client_context.current_client.blocking_queue = None

        return None

    def calculate_context(
        self,
        database: Database,
        keys_to_ids: dict[bytes, bytes],
        waiting_context: StreamWaitingContext,
    ) -> bool:
        had_keys = False
        for key, id_ in keys_to_ids.items():
            value = database.stream_database.get_value_or_empty(key)

            if id_ == StreamSpecialIds.NEW_ENTRY_ID:
                if value.entries:
                    waiting_context.keys_to_minimum_id[key] = value.entries.peekitem(-1)[0]
                    keys_to_ids[key] = _format_entry_id(value.entries.peekitem(-1)[0])
                else:
                    waiting_context.keys_to_minimum_id[key] = (0, 0)
                    keys_to_ids[key] = _format_entry_id((0, 0))
                continue
            if id_ == StreamSpecialIds.LAST_ENTRY_ID:
                if value.entries:
                    waiting_context.keys_to_minimum_id[key] = _decrease_entry_id(value.entries.peekitem()[0])
                    had_keys = True
                else:
                    waiting_context.keys_to_minimum_id[key] = (0, 0)
                continue

            try:
                timestamp, sequence = _parse_strict_entry_id(id_, sequence_fill=0)
            except ValueError:
                raise ServerError(b"ERR wrong type of argument for 'xread' command")
            waiting_context.keys_to_minimum_id[key] = (timestamp, sequence)
            if value.after((timestamp, sequence)) is not None:
                had_keys = True
                continue
        return had_keys

    async def wait_for_stream(
        self,
        client_context: ClientContext,
        keys_to_ids: dict[bytes, bytes],
        block_milliseconds: int | float | None = None,
        in_multi: bool = False,
    ) -> dict[bytes, EntryID] | None:
        client_id = client_context.current_client.client_id

        print(f"{client_id} waiting for streams {keys_to_ids.keys()} for {block_milliseconds} ms")

        database = client_context.database

        stream_waiting_context = StreamWaitingContext()
        had_keys = self.calculate_context(database, keys_to_ids, stream_waiting_context)

        if had_keys:
            return stream_waiting_context.keys_to_minimum_id

        if in_multi or block_milliseconds is None:
            return None

        client_context.current_client.blocking_queue = Queue()
        self.notifications.add_multiple(keys_to_ids.keys(), client_context.current_client.blocking_queue)

        deadline = (time.time_ns() + (block_milliseconds * 1_000_000)) if block_milliseconds != 0 else None
        try:
            while True:
                start_time = time.time_ns()
                if deadline is not None and start_time >= deadline:
                    break

                print(f"{client_id} time is {start_time}, deadline is {deadline}")

                timeout = ((deadline - start_time) / 1_000_000_000) if deadline is not None else None

                print(f"{client_id} waiting queue for keys {keys_to_ids.keys()} with timeout {timeout}")

                try:
                    queue_item = await wait_for(client_context.current_client.blocking_queue.get(), timeout=timeout)

                    if queue_item == UnblockMessage.ERROR:
                        print(f"{client_id} got unblock error from queue")
                        raise ServerError(b"UNBLOCKED client unblocked via CLIENT UNBLOCK")
                    if queue_item == UnblockMessage.TIMEOUT:
                        print(f"{client_id} got unblock timeout from queue")
                        raise TimeoutError()

                    key = queue_item

                    print(f"{client_id} got queue item for stream '{key.decode()}', recalculating context")
                    stream_waiting_context.reset()
                    try:
                        if self.calculate_context(database, keys_to_ids, stream_waiting_context):
                            return stream_waiting_context.keys_to_minimum_id
                    except ServerWrongTypeError:
                        continue
                except TimeoutError:
                    if time.time_ns() == start_time:
                        print(f"{client_id} got timeout, sleeping for 1 ms")
                        time.sleep(0.001)
                    continue
        finally:
            self.notifications.remove_all(client_context.current_client.blocking_queue)
            client_context.current_client.blocking_queue = None

        return None

    async def notify_deleted(self, key: bytes, in_multi: bool = False) -> None:
        if in_multi:
            if key in self.lazy_notification_keys:
                if self.lazy_notification_keys[key].created_in_transaction:
                    print(f"notified key '{key.decode()}' was created and deleted in the same transaction, removing...")
                    del self.lazy_notification_keys[key]
                else:
                    print(f"marking lazy notified key '{key.decode()}' as deleted")
                    self.lazy_notification_keys[key].deleted = True
            else:
                print(f"adding deleted '{key.decode()}' to lazy notification keys")
                self.lazy_notification_keys[key] = LazyNotification(key, deleted=True, created_in_transaction=True)
            return
        for queue in self.notifications.iter_values(key):
            print(f"putting stream '{key.decode()}' into queue")
            await queue.put(key)

    async def notify(self, key: bytes, in_multi: bool = False) -> None:
        if in_multi:
            if key in self.lazy_notification_keys:
                print(f"notified lazy key '{key.decode()}' was recreated, marking as not deleted")
                self.lazy_notification_keys[key].deleted = False
            else:
                print(f"adding '{key.decode()}' to lazy notification keys")
                self.lazy_notification_keys[key] = LazyNotification(key, deleted=False, created_in_transaction=in_multi)
            return
        for queue in self.notifications.iter_values(key):
            print(f"putting '{key.decode()}' into queue")
            await queue.put(key)

    async def notify_safely(self, database: Database, key: bytes, in_multi: bool = False) -> None:
        try:
            if self.has_key(database, key):
                await self.notify(key, in_multi=in_multi)
        except ServerWrongTypeError:
            pass

    async def notify_lazy(self, database: Database) -> None:
        while self.lazy_notification_keys:
            key, lazy_notification = self.lazy_notification_keys.popitem(last=False)

            if (not lazy_notification.deleted) and (
                (key not in database.content.data) or not self.is_instance(database.content.data[key].value)
            ):
                print(f"lazy key '{key.decode()}' not found, continue...")
                continue
            for queue in self.notifications.iter_values(key):
                print(f"putting '{key.decode()}' into queue")
                await queue.put(key)


@dataclass
class BlockingManager:
    list_blocking_manager: ListBlockingManager = field(default_factory=ListBlockingManager)
    sorted_set_blocking_manager: SortedSetBlockingManager = field(default_factory=SortedSetBlockingManager)
    stream_blocking_manager: StreamBlockingManager = field(default_factory=StreamBlockingManager)

    async def notify_safely(
        self,
        database: Database,
        key: bytes,
        in_multi: bool = False,
    ) -> None:
        await self.list_blocking_manager.notify_safely(database, key, in_multi=in_multi)
        await self.sorted_set_blocking_manager.notify_safely(database, key, in_multi=in_multi)
        await self.stream_blocking_manager.notify_safely(database, key, in_multi=in_multi)

    async def notify_safely_all(self, database: Database, in_multi: bool = False) -> None:
        for key in database.keys():
            await self.list_blocking_manager.notify_safely(database, key, in_multi=in_multi)
            await self.stream_blocking_manager.notify_safely(database, key, in_multi=in_multi)


@dataclass(unsafe_hash=True)
class ClientWatchlist:
    watchlist: dict[tuple[int, bytes], bool] = field(default_factory=dict, hash=False)
