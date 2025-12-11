import fnmatch
import json
import random
from dataclasses import field
from typing import Any

from pyvalkey.blocking import BlockingManager, StreamBlockingManager
from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command, DatabaseCommand
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import is_integer
from pyvalkey.consts import LONG_LONG_MAX, LONG_LONG_MIN
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import (
    Database,
    KeyValue,
)
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.database_objects.scored_sorted_set import ScoredSortedSet
from pyvalkey.database_objects.stream import Consumer, ConsumerGroup, Stream
from pyvalkey.enums import NotificationType
from pyvalkey.listpack import listpack
from pyvalkey.notifications import NotificationsManager
from pyvalkey.resp import RESP_OK, ValueType
from pyvalkey.utils.times import now_ms


@command(b"copy", {b"keyspace", b"write", b"slow"})
class Copy(Command):
    client_context: ClientContext = dependency()

    source: bytes = positional_parameter(key_mode=b"R")
    destination: bytes = positional_parameter(key_mode=b"W")
    replace: bool = keyword_parameter(flag=b"REPLACE", default=False)
    db: int | None = keyword_parameter(flag=b"DB", default=None)

    def execute(self) -> ValueType:
        source_key_value = self.client_context.database.get_or_none(self.source)

        if source_key_value is None:
            return False

        database = self.client_context.server_context.get_or_create_database(
            self.client_context.current_database if self.db is None else self.db
        )

        destination_key_value = database.get_or_none(self.destination)

        if destination_key_value is None:
            database.copy_from(source_key_value, self.destination)
            return True

        if not self.replace:
            return False

        database.pop(self.destination)
        database.copy_from(source_key_value, self.destination)
        return True


@command(b"del", {b"keyspace", b"write", b"slow"})
class Delete(DatabaseCommand):
    notifications: NotificationsManager = dependency()

    blocking_manager: StreamBlockingManager = dependency()
    keys: list[bytes] = positional_parameter()

    _stream_keys: list[bytes] = field(default_factory=list, init=False)

    def execute(self) -> ValueType:
        count = 0
        for key in self.keys:
            value = self.database.pop(key, None)
            if value is not None:
                count += 1
                self.notifications.notify(NotificationType.GENERIC, b"del", key)
                if isinstance(value.value, Stream):
                    self._stream_keys.append(key)

        return count

    async def after(self, in_multi: bool = False) -> None:
        for key in self._stream_keys:
            await self.blocking_manager.notify_deleted(key, in_multi=in_multi)


@command(b"delifeq", {b"keyspace", b"write", b"slow"})
class DeleteIdEqual(DatabaseCommand):
    key: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.get_or_none(self.key)
        if key_value is None or key_value.value != self.value:
            return 0

        self.database.pop(self.key)
        return 1


@command(b"dump", {b"keyspace", b"read", b"slow"})
class Dump(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None

        dump_value: dict[str, Any] = {
            "value": key_value.value,
        }
        if isinstance(key_value.value, list):
            dump_value = {
                "type": "list",
                "value": [item.decode() for item in key_value.value],
            }
        elif isinstance(key_value.value, set):
            dump_value["type"] = "set"
        elif isinstance(key_value.value, dict):
            dump_value = {
                "type": "hash",
                "value": {
                    k.decode(): (v.decode() if isinstance(v, bytes) else str(v)) for k, v in key_value.value.items()
                },
            }
        elif isinstance(key_value.value, int):
            dump_value["type"] = "int"
        elif isinstance(key_value.value, ScoredSortedSet):
            dump_value = {
                "type": "sorted_set",
                "value": key_value.value.members,
            }
        elif isinstance(key_value.value, bytes):
            dump_value = {
                "type": "string",
                "value": key_value.value.decode(),
            }
        elif isinstance(key_value.value, Stream):
            dump_value = {
                "type": "stream",
                "value": key_value.value.dump(),
            }
        else:
            raise TypeError()
        return json.dumps(dump_value).encode()


@command(b"exists", {b"read", b"string", b"fast"})
class Exists(DatabaseCommand):
    keys: list[bytes] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return sum(1 for key in self.keys if self.database.get_or_none(key) is not None)


@command(b"expire", {b"keyspace", b"write", b"fast"})
class Expire(DatabaseCommand):
    notifications: NotificationsManager = dependency()

    key: bytes = positional_parameter()
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        expire_set = self.database.set_expiration_in(self.key, self.seconds * 1000)
        if self.database.get_or_none(self.key) is not None:
            self.notifications.notify(NotificationType.GENERIC, b"expire", self.key)
        return expire_set


@command(b"expireat", {b"keyspace", b"write", b"fast"})
class ExpireAt(DatabaseCommand):
    key: bytes = positional_parameter()
    timestamp: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_at(self.key, self.timestamp * 1000)


@command(b"expiretime", {b"keyspace", b"write", b"fast"})
class Expiration(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        expiration = self.database.get_expiration(self.key)
        return expiration // 1000 if expiration is not None else None


@command(b"keys", {b"keyspace", b"read", b"slow", b"dangerous"})
class Keys(DatabaseCommand):
    MAXIMUM_NESTING = 1000

    pattern: bytes = positional_parameter()

    @classmethod
    def nesting(cls, pattern: bytes) -> int:
        return sum(1 for p in pattern.split(b"*")[1:] if p)

    def execute(self) -> ValueType:
        if self.nesting(self.pattern) > self.MAXIMUM_NESTING:
            return []
        return list(fnmatch.filter(self.database.keys(), self.pattern))


@command(b"migrate", {b"keyspace", b"read", b"slow", b"dangerous"})
class Migrate(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


@command(b"move", {b"keyspace", b"write", b"slow", b"dangerous"})
class Move(Command):
    database: Database = dependency()
    server_context: ServerContext = dependency()

    key: bytes = positional_parameter()
    db: int = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.pop(self.key)

        if key_value is None:
            return True

        database = self.server_context.get_or_create_database(self.db)

        if database.has_key(self.key):
            return False
        database.set_key_value(key_value)

        return True


@command(b"encoding", {b"read", b"keyspace", b"slow"}, parent_command=b"object")
class ObjectEncoding(DatabaseCommand):
    configuration: Configurations = dependency()

    key: bytes = positional_parameter(key_mode=b"R")

    @classmethod
    def approximate_list_size(cls, value: list[bytes]) -> int:
        return sum(len(item) for item in value)

    def is_list_listpack(self, value: list[bytes]) -> bool:
        list_max_listpack_size = self.configuration.list_max_listpack_size

        if list_max_listpack_size >= 0:
            list_max_listpack_size = list_max_listpack_size or 1  # Fix 0 to be 1
            if len(value) <= list_max_listpack_size:
                return True

        else:
            list_max_listpack_size = max(list_max_listpack_size, -5)  # Fix lower than -5 to be -5

            listpack_value = listpack()
            for item in value:
                listpack_value.append(item)

            if listpack_value.total_bytes <= ((2 * (2 ** abs(list_max_listpack_size))) * 1024):
                return True

        return False

    def is_set_intset(self, value: set[bytes]) -> bool:
        if len(value) > self.configuration.set_max_intset_entries:
            return False
        for item in value:
            if not is_integer(item):
                return False
            else:
                int_item = int(item)
                if int_item > LONG_LONG_MAX or int_item < LONG_LONG_MIN:
                    return False
        return True

    def is_sorted_set_listpack(self, value: ScoredSortedSet) -> bool:
        if len(value) > self.configuration.zset_max_listpack_entries:
            return False
        if max(8 + len(m) for s, m in value.members) > self.configuration.zset_max_listpack_value:
            return False
        return True

    def is_dict_listpack(self, value: dict[bytes, bytes | int]) -> bool:
        if len(value) > self.configuration.hash_max_listpack_entries:
            return False

        maximum_value = 0
        for k, v in value.items():
            maximum_value = max(maximum_value, len(k), len(v if isinstance(v, bytes) else str(v)))

        if maximum_value > self.configuration.hash_max_listpack_value:
            return False
        return True

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None
        if isinstance(key_value.value, list):
            if self.is_list_listpack(key_value.value):
                return b"listpack"
            return b"quicklist"

        if isinstance(key_value.value, set):
            if self.is_set_intset(key_value.value):
                return b"intset"
            if len(key_value.value) <= self.configuration.set_max_listpack_entries and all(
                map(lambda k: len(k) <= self.configuration.set_max_listpack_value, key_value.value)
            ):
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, ScoredSortedSet):
            if self.is_sorted_set_listpack(key_value.value):
                return b"listpack"
            return b"skiplist"

        if isinstance(key_value.value, dict):
            if self.is_dict_listpack(key_value.value):
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, int):
            return b"int"

        return b"raw"


@command(b"idletime", {b"read", b"keyspace", b"slow"}, parent_command=b"object")
class ObjectFrequency(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None
        return now_ms() - key_value.last_accessed


@command(b"idletime", {b"read", b"keyspace", b"slow"}, parent_command=b"object")
class ObjectIdleTime(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None
        return now_ms() - key_value.last_accessed


@command(b"persist", {b"keyspace", b"write", b"fast"})
class Persist(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        return self.database.set_persist(self.key)


@command(b"pexpire", {b"keyspace", b"write", b"fast"})
class ExpireMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter()
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_in(self.key, self.seconds)


@command(b"pexpireat", {b"keyspace", b"write", b"fast"})
class ExpireAtMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter()
    timestamp: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_at(self.key, self.timestamp)


@command(b"pexpiretime", {b"keyspace", b"write", b"fast"})
class ExpirationMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        return self.database.get_expiration(self.key)


@command(b"pttl", {b"read", b"keyspace", b"fast"})
class TimeToLiveMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_time_to_live(self.key)
            if expiration is None:
                return -1
            return expiration
        except KeyError:
            return -2


@command(b"randomkey", {b"keyspace", b"write", b"slow", b"dangerous"})
class RandomKey(Command):
    database: Database = dependency()

    def execute(self) -> ValueType:
        if self.database.empty():
            return None

        fully_volatile = self.database.are_fully_volatile()
        max_tries = 100 if fully_volatile else None

        while True:
            random_key: bytes = random.choice(list(self.database.keys()))

            if self.database.has_key(random_key):
                break

            if max_tries is None:
                continue

            if max_tries == 0:
                break

            max_tries -= 1

        return random_key


@command(b"rename", {b"keyspace", b"write", b"slow"})
class Rename(Command):
    database: Database = dependency()
    blocking_manager: BlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            raise ServerError(b"ERR no such key")

        self.database.rename_unsafely(self.key, self.new_key)

        return RESP_OK

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify_safely(self.database, self.new_key, in_multi=in_multi)


@command(b"renamenx", {b"keyspace", b"write", b"slow"})
class RenameIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            raise ServerError(b"ERR no such key")
        if self.database.has_key(self.new_key):
            return 0

        self.database.rename_unsafely(self.key, self.new_key)

        return 1


@command(b"restore", {b"keyspace", b"write", b"slow", b"dangerous"})
class Restore(DatabaseCommand):
    key: bytes = positional_parameter()
    ttl: int = positional_parameter()
    serialized_value: bytes = positional_parameter()
    replace: bool = keyword_parameter(flag=b"REPLACE")
    absolute_ttl: bool = keyword_parameter(flag=b"ABSTTL")
    idle_time_seconds: int | None = keyword_parameter(default=None, token=b"IDLETIME")
    frequency: int | None = keyword_parameter(default=None, token=b"FREQ")

    def execute(self) -> ValueType:
        try:
            json_value: dict[str, Any] = json.loads(self.serialized_value)
        except UnicodeDecodeError:
            if self.serialized_value[0] in (15, 19, 21):
                s = Stream()
                g = s.consumer_groups[b"g"] = ConsumerGroup(b"g", last_id=(0, 0))
                g.consumers[b"c"] = Consumer(b"c")
                self.database.set_key_value(
                    KeyValue(
                        self.key,
                        s,
                    )
                )

            return RESP_OK

        if json_value["type"] == "hash":
            value = json_value["value"]
        elif json_value["type"] == "set":
            value = set(json_value["value"])
        elif json_value["type"] == "list":
            value = [item.encode() for item in json_value["value"]]
        elif json_value["type"] == "sorted_set":
            value = ScoredSortedSet([(score, member) for score, member in json_value["value"]])
        elif json_value["type"] == "string":
            value = json_value["value"].encode()
        elif json_value["type"] == "int":
            value = json_value["value"]
        elif json_value["type"] == "stream":
            value = Stream.restore(json_value["value"])
        else:
            raise ServerError(b"ERR DUMP payload version or checksum are wrong")

        if not self.replace and self.database.has_key(self.key):
            raise ServerError(b"BUSYKEY Target key name already exists.")

        kwargs = {}
        if self.idle_time_seconds:
            kwargs["last_accessed"] = self.idle_time_seconds
        if self.ttl:
            kwargs["expiration"] = (now_ms() + self.ttl) if not self.absolute_ttl else self.ttl

        self.database.set_key_value(
            KeyValue(
                self.key,
                value,
                **kwargs,
            )
        )

        return RESP_OK


@command(b"scan", {b"keyspace", b"write", b"slow", b"dangerous"})
class Scan(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"sort", {b"read", b"set", b"sortedset", b"list", b"slow", b"dangerous"})
class Sort(Command):
    database: Database = dependency()
    blocking_manager: BlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = keyword_parameter(flag=b"ALPHA", default=False)
    destination: bytes | None = keyword_parameter(skip_first=True, token=b"STORE", default=None, key_mode=b"W")

    def execute(self) -> ValueType:
        result_values = SortReadOnly(
            database=self.database,
            key=self.key,
            by=self.by,
            limit=self.limit,
            get_values=self.get_values,
            descending=self.descending,
            alpha=self.alpha,
        ).internal_execute()

        if self.destination is None:
            return result_values

        if not result_values:
            self.database.pop(self.destination, None)
            return 0

        self.database.set_key_value(
            KeyValue(self.destination, [(v if v is not None else b"") for v in result_values]),
        )
        return len(result_values)

    async def after(self, in_multi: bool = False) -> None:
        if self.destination is None:
            return
        await self.blocking_manager.notify_safely(self.database, self.destination, in_multi=in_multi)


@command(b"sort_ro", {b"write", b"set", b"sortedset", b"list", b"slow", b"dangerous"})
class SortReadOnly(Command):
    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = keyword_parameter(flag=b"ALPHA", default=False)

    def _get_referenced_value(self, value: bytes, reference: bytes) -> int | bytes | None:
        reference_key, reference_field = reference, None
        if b"->" in reference_key and not reference_key.endswith(b"->"):
            reference_key, reference_field = reference.rsplit(b"->", 1)

        key_value = self.database.get_or_none(reference_key.replace(b"*", value, 1))
        if key_value is None:
            return None
        if reference_field:
            if isinstance(key_value.value, dict):
                return key_value.value[reference_field]
            return None
        if isinstance(key_value.value, int):
            return key_value.value
        if not isinstance(key_value.value, bytes):
            return None
        return key_value.value

    def internal_execute(self) -> list[int | bytes | None] | None:
        key_value = self.database.get_or_none(self.key)

        if key_value is None:
            return None

        if not isinstance(key_value.value, list | set | ScoredSortedSet):
            raise ServerWrongTypeError()

        values: list[bytes]
        if isinstance(key_value.value, ScoredSortedSet):
            values = [member for score, member in key_value.value.members]
        else:
            values = list(key_value.value)

        if self.by != b"nosort":
            referenced_values: dict[bytes, bytes | int] | None = None
            if self.by is not None:
                referenced_values = {}
                for value in values:
                    referenced_value = self._get_referenced_value(value, self.by)
                    if referenced_value is not None:
                        referenced_values[value] = referenced_value

            scores: dict[bytes, float | bytes] = {v: v for v in values}
            for value in values:
                if not self.alpha:
                    try:
                        scores[value] = float(
                            referenced_values.get(value, 0) if referenced_values is not None else value
                        )
                    except ValueError:
                        raise ServerError(b"ERR One or more scores can't be converted into double")
                else:
                    scores[value] = referenced_values.get(value, value) if referenced_values is not None else value

            values.sort(key=lambda v: (scores[v], v), reverse=self.descending)
        elif self.descending:
            values.reverse()

        if self.limit is not None:
            offset, count = self.limit
            values = values[offset : offset + count]

        result_values: list[int | bytes | None] = list(values)
        if self.get_values:
            get_result: list[int | bytes | None] = []
            for value in values:
                for get_value in self.get_values:
                    if get_value == b"#":
                        get_result.append(value)
                        continue
                    get_result.append(self._get_referenced_value(value, get_value))
            result_values = get_result

        return result_values

    def execute(self) -> ValueType:
        return self.internal_execute()


@command(b"touch", {b"keyspace", b"write", b"slow", b"dangerous"})
class Touch(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"ttl", {b"read", b"keyspace", b"fast"})
class TimeToLive(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_time_to_live(self.key)
            if expiration is None:
                return -1
            return expiration // 1000
        except KeyError:
            return -2


@command(b"type", {b"keyspace", b"write", b"slow", b"dangerous"})
class Type(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.get_value_or_none(self.key)
        if value is None:
            return b"none"
        if isinstance(value, bytes):
            return b"string"
        if isinstance(value, list):
            return b"list"
        if isinstance(value, ScoredSortedSet):
            return b"sorted_set"

        raise TypeError(f"not supporting type {type(value)}")


@command(b"unlink", {b"keyspace", b"write", b"slow", b"dangerous"})
class Unlink(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"wait", {b"keyspace", b"write", b"slow", b"dangerous"})
class Wait(DatabaseCommand):
    numreplicas: int = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        return 0


@command(b"waitaof", {b"keyspace", b"write", b"slow", b"dangerous"})
class WaitAOF(DatabaseCommand):
    numlocal: int = positional_parameter()
    numreplicas: int = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        return [0, 0]
