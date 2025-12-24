from __future__ import annotations

import time
import typing
from asyncio import wait_for
from collections import OrderedDict
from dataclasses import dataclass, field

from pyvalkey.commands.utils import _decrease_entry_id, _format_entry_id, _parse_strict_entry_id
from pyvalkey.database_objects.clients import BlockingContext
from pyvalkey.database_objects.databases import Database, KeyValueTypeVar
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.database_objects.scored_sorted_set import ScoredSortedSet
from pyvalkey.database_objects.stream import Consumer, ConsumerGroup, EntryID, Stream
from pyvalkey.enums import NotificationType, StreamSpecialIds, UnblockMessage
from pyvalkey.utils.collections import OrderedBiMap

if typing.TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext


@dataclass
class BlockingManagerBase:
    notifications: OrderedBiMap[bytes, BlockingContext] = field(default_factory=OrderedBiMap)
    lazy_notification_keys: list[bytes] = field(default_factory=list)

    def has_key(self, database: Database, key: bytes) -> bool:
        raise NotImplementedError()

    def is_instance(self, value: KeyValueTypeVar) -> bool:
        raise NotImplementedError()

    async def wait_for_lists(
        self,
        client_context: ClientContext,
        command: bytes,
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

        client_context.current_client.blocking_context = BlockingContext(command)
        self.notifications.add_multiple(keys, client_context.current_client.blocking_context)

        try:
            while True:
                print(f"{client_context.current_client.client_id} waiting queue for keys {keys}")
                message: bytes | UnblockMessage = await wait_for(
                    client_context.current_client.blocking_context.queue.get(), timeout=timeout or None
                )
                if message == UnblockMessage.ERROR:
                    print(f"{client_context.current_client.client_id} got unblock error from queue")
                    raise ServerError(b"UNBLOCKED client unblocked via CLIENT UNBLOCK")
                if message == UnblockMessage.TIMEOUT:
                    print(f"{client_context.current_client.client_id} got unblock timeout from queue")
                    raise TimeoutError()
                print(f"{client_context.current_client.client_id} got '{message.decode()}' from queue")
                if self.has_key(client_context.database, message):
                    return message
                print(
                    f"{client_context.current_client.client_id} key '{message.decode()}' not in database, continue..."
                )
        except TimeoutError:
            return None
        finally:
            self.notifications.remove_all(client_context.current_client.blocking_context)
            if clear_queue:
                client_context.current_client.blocking_context = None

    async def notify(self, key: bytes, in_multi: bool = False) -> None:
        if in_multi:
            print(f"adding '{key.decode()}' to lazy notification keys")
            self.lazy_notification_keys.append(key)
            return
        for blocking_context in self.notifications.iter_values(key):
            print(f"putting '{key.decode()}' into queue")
            await blocking_context.queue.put(key)

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
            for blocking_context in self.notifications.iter_values(key):
                print(f"putting '{key.decode()}' into queue")
                await blocking_context.queue.put(key)


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
    notifications: OrderedBiMap[bytes, BlockingContext] = field(default_factory=OrderedBiMap)
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
                database.notify(NotificationType.STREAM, b"xgroup-createconsumer", key)
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

        client_context.current_client.blocking_context = BlockingContext(b"xreadgroup")
        self.notifications.add_multiple(keys_to_ids.keys(), client_context.current_client.blocking_context)

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
                    queue_item = await wait_for(
                        client_context.current_client.blocking_context.queue.get(), timeout=timeout
                    )

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
            self.notifications.remove_all(client_context.current_client.blocking_context)
            client_context.current_client.blocking_context = None

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
                if len(value) > 0:
                    last_entry_id = value.last_id
                    waiting_context.keys_to_minimum_id[key] = last_entry_id
                    keys_to_ids[key] = _format_entry_id(last_entry_id)
                else:
                    waiting_context.keys_to_minimum_id[key] = (0, 0)
                    keys_to_ids[key] = _format_entry_id((0, 0))
                continue
            if id_ == StreamSpecialIds.LAST_ENTRY_ID:
                if len(value) > 0:
                    waiting_context.keys_to_minimum_id[key] = _decrease_entry_id(value.last_id)
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
        had_keys = self.calculate_context(client_context.database, keys_to_ids, stream_waiting_context)

        if had_keys:
            return stream_waiting_context.keys_to_minimum_id

        if in_multi or block_milliseconds is None:
            return None

        client_context.current_client.blocking_context = BlockingContext(b"xread")
        self.notifications.add_multiple(keys_to_ids.keys(), client_context.current_client.blocking_context)

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
                    queue_item = await wait_for(
                        client_context.current_client.blocking_context.queue.get(), timeout=timeout
                    )

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
            self.notifications.remove_all(client_context.current_client.blocking_context)
            client_context.current_client.blocking_context = None

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
        for blocking_context in self.notifications.iter_values(key):
            print(f"putting stream '{key.decode()}' into queue")
            await blocking_context.queue.put(key)

    async def notify(self, key: bytes, in_multi: bool = False) -> None:
        if in_multi:
            if key in self.lazy_notification_keys:
                print(f"notified lazy key '{key.decode()}' was recreated, marking as not deleted")
                self.lazy_notification_keys[key].deleted = False
            else:
                print(f"adding '{key.decode()}' to lazy notification keys")
                self.lazy_notification_keys[key] = LazyNotification(key, deleted=False, created_in_transaction=in_multi)
            return
        for blocking_context in self.notifications.iter_values(key):
            print(f"putting '{key.decode()}' into queue")
            await blocking_context.queue.put(key)

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
            for blocking_context in self.notifications.iter_values(key):
                print(f"putting '{key.decode()}' into queue")
                await blocking_context.queue.put(key)


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

    def total_key_blocking(self) -> int:
        return len(
            self.list_blocking_manager.notifications.mapping.keys()
            | self.sorted_set_blocking_manager.notifications.mapping.keys()
            | self.stream_blocking_manager.notifications.mapping.keys()
        )

    def total_key_blocking_on_no_keys(self) -> int:
        total_number = 0

        for blocking_contexts in self.stream_blocking_manager.notifications.mapping.values():
            has_xreadgroup = False
            for blocking_context in blocking_contexts:
                if blocking_context.command == b"xreadgroup":
                    has_xreadgroup = True
                    break
            if has_xreadgroup:
                total_number += 1

        return total_number
