import fnmatch
import json
import random
from dataclasses import field
from typing import Any, cast

from pyvalkey.blocking import BlockingManager, StreamBlockingManager
from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import is_integer
from pyvalkey.consts import LONG_LONG_MAX, LONG_LONG_MIN
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import (
    Database,
    KeyValue,
)
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.database_objects.information import Information
from pyvalkey.database_objects.scored_sorted_set import ScoredSortedSet
from pyvalkey.database_objects.stream import Consumer, ConsumerGroup, Stream
from pyvalkey.enums import NotificationType
from pyvalkey.listpack import listpack
from pyvalkey.notifications import NotificationsManager
from pyvalkey.resp import RESP_OK, ValueType
from pyvalkey.utils.dependencies import dependency
from pyvalkey.utils.times import now_ms


@command(b"copy", {b"keyspace", b"slow"}, flags={b"denyoom", b"write"})
class Copy(Command):
    """
    summary: Copies the value of a key to a new key.
    complexity: >-
      O(N) worst case for collections, where N is the number of nested items. O(1) for string values.
    since: 6.2.0
    function: copyCommand
    reply_schema:
      oneOf:
      - description: Source was copied.
        const: 1
      - description: Source was not copied when the destination key already exists
        const: 0
    """

    client_context: ClientContext = dependency()

    source: bytes = positional_parameter(key_mode=b"R")
    destination: bytes = positional_parameter(key_mode=b"W")
    replace: bool = flag_parameter(token=b"REPLACE")
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


@command(b"del", {b"keyspace", b"slow"}, flags={b"write"})
class Delete(Command):
    """
    summary: Deletes one or more keys.
    complexity: >-
      O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string,
      the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted
      set or hash. Removing a single key that holds a string value is O(1).
    since: 1.0.0
    function: delCommand
    reply_schema:
      description: The number of keys that were removed.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
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


@command(b"delifeq", {b"string"}, flags={b"fast", b"write"})
class DeleteIdEqual(Command):
    """
    summary: Delete key if value matches string.
    complexity: O(1)
    since: 9.0.0
    function: delifeqCommand
    reply_schema:
      oneOf:
      - description: The key was not deleted.
        const: 0
      - description: The key was deleted.
        const: 1
    """

    database: Database = dependency()
    key: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.get_or_none(self.key)
        if key_value is None or key_value.value != self.value:
            return 0

        self.database.pop(self.key)
        return 1


@command(b"dump", {b"keyspace", b"read", b"slow"}, flags={b"readonly"})
class Dump(Command):
    """
    summary: Returns a serialized representation of the value stored at a key.
    complexity: >-
      O(1) to access the key and additional O(N*M) to serialize it, where N is the number of objects composing the
      value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is
      small, so simply O(1).
    since: 2.6.0
    function: dumpCommand
    reply_schema:
      oneOf:
      - description: The serialized value.
        type: string
      - description: Key does not exist.
        type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None

        dump_value: dict[str, Any] = {
            "value": key_value.value,
        }
        if isinstance(key_value.value, list):
            list_value = cast("list[bytes]", key_value.value)
            dump_value = {
                "type": "list",
                "value": [item.decode() for item in list_value],
            }
        elif isinstance(key_value.value, set):
            dump_value["type"] = "set"
        elif isinstance(key_value.value, dict):
            dict_value = cast("dict[bytes, bytes | int]", key_value.value)
            dump_value = {
                "type": "hash",
                "value": {k.decode(): (v.decode() if isinstance(v, bytes) else str(v)) for k, v in dict_value.items()},
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


@command(b"exists", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class Exists(Command):
    """
    summary: Determines whether one or more keys exist.
    complexity: O(N) where N is the number of keys to check.
    since: 1.0.0
    function: existsCommand
    reply_schema:
      description: Number of keys that exist from those specified as arguments.
      type: integer
    """

    database: Database = dependency()
    keys: list[bytes] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return sum(1 for key in self.keys if self.database.get_or_none(key) is not None)


@command(b"expire", {b"keyspace"}, flags={b"fast", b"write"})
class Expire(Command):
    """
    summary: Sets the expiration time of a key in seconds.
    complexity: O(1)
    since: 1.0.0
    function: expireCommand
    reply_schema:
      oneOf:
      - description: >-
          The timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.
        const: 0
      - description: The timeout was set.
        const: 1
    """

    database: Database = dependency()
    notifications: NotificationsManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        expire_set = self.database.set_expiration_in(self.key, self.seconds * 1000)
        if self.database.get_or_none(self.key) is not None:
            self.notifications.notify(NotificationType.GENERIC, b"expire", self.key)
        return expire_set


@command(b"expireat", {b"keyspace"}, flags={b"fast", b"write"})
class ExpireAt(Command):
    """
    summary: Sets the expiration time of a key to a Unix timestamp.
    complexity: O(1)
    since: 1.2.0
    function: expireatCommand
    reply_schema:
      oneOf:
      - const: 1
        description: The timeout was set.
      - const: 0
        description: >-
          The timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    timestamp: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_at(self.key, self.timestamp * 1000)


@command(b"expiretime", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class Expiration(Command):
    """
    summary: Returns the expiration time of a key as a Unix timestamp.
    complexity: O(1)
    since: 7.0.0
    function: expiretimeCommand
    reply_schema:
      oneOf:
      - type: integer
        description: Expiration Unix timestamp in seconds.
        minimum: 0
      - const: -1
        description: The key exists but has no associated expiration time.
      - const: -2
        description: The key does not exist.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        expiration = self.database.get_expiration(self.key)
        return expiration // 1000 if expiration is not None else None


@command(b"keys", {b"dangerous", b"keyspace", b"read", b"slow"}, flags={b"readonly"})
class Keys(Command):
    """
    summary: Returns all key names that match a pattern.
    complexity: >-
      O(N) with N being the number of keys in the database, under the assumption that the key names in the database
      and the given pattern have limited length.
    since: 1.0.0
    function: keysCommand
    reply_schema:
      description: List of keys matching pattern.
      type: array
      items:
        type: string
    """

    database: Database = dependency()
    MAXIMUM_NESTING = 1000

    pattern: bytes = positional_parameter()

    @classmethod
    def nesting(cls, pattern: bytes) -> int:
        return sum(1 for p in pattern.split(b"*")[1:] if p)

    def execute(self) -> ValueType:
        if self.nesting(self.pattern) > self.MAXIMUM_NESTING:
            return []
        return list(fnmatch.filter(self.database.keys(), self.pattern))


@command(b"migrate", {b"dangerous", b"keyspace", b"slow"}, flags={b"write"})
class Migrate(Command):
    """
    summary: Atomically transfers a key from one instance to another.
    complexity: >-
      This command actually executes a DUMP+DEL in the source instance, and a RESTORE in the target instance. See
      the pages of these commands for time complexity. Also an O(N) data transfer between the two instances is
      performed.
    since: 2.6.0
    function: migrateCommand
    reply_schema:
      oneOf:
      - const: OK
        description: Success.
      - const: NOKEY
        description: No keys were found in the source instance.
    """

    database: Database = dependency()

    def execute(self) -> ValueType:
        return None


@command(b"move", {b"keyspace"}, flags={b"fast", b"write"})
class Move(Command):
    """
    summary: Moves a key to another database.
    complexity: O(1)
    since: 1.0.0
    function: moveCommand
    reply_schema:
      oneOf:
      - description: Key was moved.
        const: 1
      - description: >-
          Key wasn't moved. When key already exists in the destination database, or it does not exist in the source
          database
        const: 0
    """

    database: Database = dependency()
    server_context: ServerContext = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
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


@command(b"encoding", {b"keyspace", b"read", b"slow"}, parent_command=b"object", flags={b"readonly"})
class ObjectEncoding(Command):
    """
    summary: Returns the internal encoding of an object.
    complexity: O(1)
    since: 2.2.3
    function: objectCommand
    reply_schema:
      oneOf:
      - description: Key doesn't exist.
        type: 'null'
      - description: Encoding of the object.
        type: string
    """

    database: Database = dependency()
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
            list_value = cast("list[bytes]", key_value.value)
            if self.is_list_listpack(list_value):
                return b"listpack"
            return b"quicklist"

        if isinstance(key_value.value, set):
            set_value = cast("set[bytes]", key_value.value)
            if self.is_set_intset(set_value):
                return b"intset"
            if len(set_value) <= self.configuration.set_max_listpack_entries and all(
                map(lambda k: len(k) <= self.configuration.set_max_listpack_value, set_value)
            ):
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, ScoredSortedSet):
            if self.is_sorted_set_listpack(key_value.value):
                return b"listpack"
            return b"skiplist"

        if isinstance(key_value.value, dict):
            dict_value = cast("dict[bytes, bytes | int]", key_value.value)
            if self.is_dict_listpack(dict_value):
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, int):
            return b"int"

        return b"raw"


@command(b"help", {b"keyspace", b"slow"}, parent_command=b"object", flags={b"loading", b"stale"})
class ObjectHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 6.2.0
    function: objectCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"ENCODING <key>",
            b"    Return the internal encoding for the Redis object associated with the <key>.",
            b"FREQ <key>",
            b"    Return the LFU counter for the object associated with the <key>.",
            b"IDLETIME <key>",
            b"    Return the number of seconds since the object stored at the specified key is idle.",
            b"REFCOUNT <key>",
            b"    Return the number of references of the value associated with the specified key.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"freq", {b"keyspace", b"read", b"slow"}, parent_command=b"object", flags={b"readonly"})
class ObjectFrequency(Command):
    """
    summary: Returns the logarithmic access frequency counter of an object.
    complexity: O(1)
    since: 4.0.0
    function: objectCommand
    reply_schema:
      description: The counter's value.
      type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        key_value = self.database.get_or_none(self.key)
        if key_value is None:
            return None
        return key_value.lfu_counter


@command(b"refcount", {b"keyspace", b"read", b"slow"}, parent_command=b"object", flags={b"readonly"})
class ObjectRefCount(Command):
    """
    summary: Returns the reference count of a value of a key.
    complexity: O(1)
    since: 2.2.3
    function: objectCommand
    reply_schema:
      description: The number of references.
      type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        key_value = self.database.get_or_none(self.key)
        if key_value is None:
            return None
        return 1


@command(b"idletime", {b"keyspace", b"read", b"slow"}, parent_command=b"object", flags={b"readonly"})
class ObjectIdleTime(Command):
    """
    summary: Returns the time since the last access to an object.
    complexity: O(1)
    since: 2.2.3
    function: objectCommand
    reply_schema:
      description: The idle time in seconds.
      type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        key_value = self.database.get_or_none(self.key)
        if key_value is None:
            return None
        return (now_ms() - key_value.last_accessed) // 1000


@command(b"persist", {b"keyspace"}, flags={b"fast", b"write"})
class Persist(Command):
    """
    summary: Removes the expiration time of a key.
    complexity: O(1)
    since: 2.2.0
    function: persistCommand
    reply_schema:
      oneOf:
      - const: 0
        description: Key does not exist or does not have an associated timeout.
      - const: 1
        description: The timeout has been removed.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        return self.database.set_persist(self.key)


@command(b"pexpire", {b"keyspace"}, flags={b"fast", b"write"})
class ExpireMilliseconds(Command):
    """
    summary: Sets the expiration time of a key in milliseconds.
    complexity: O(1)
    since: 2.6.0
    function: pexpireCommand
    reply_schema:
      oneOf:
      - const: 0
        description: >-
          The timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.
      - const: 1
        description: The timeout was set.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_in(self.key, self.seconds)


@command(b"pexpireat", {b"keyspace"}, flags={b"fast", b"write"})
class ExpireAtMilliseconds(Command):
    """
    summary: Sets the expiration time of a key to a Unix milliseconds timestamp.
    complexity: O(1)
    since: 2.6.0
    function: pexpireatCommand
    reply_schema:
      oneOf:
      - const: 1
        description: The timeout was set.
      - const: 0
        description: >-
          The timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    timestamp: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_at(self.key, self.timestamp)


@command(b"pexpiretime", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class ExpirationMilliseconds(Command):
    """
    summary: Returns the expiration time of a key as a Unix milliseconds timestamp.
    complexity: O(1)
    since: 7.0.0
    function: pexpiretimeCommand
    reply_schema:
      oneOf:
      - type: integer
        description: Expiration Unix timestamp in milliseconds.
        minimum: 0
      - const: -1
        description: The key exists but has no associated expiration time.
      - const: -2
        description: The key does not exist.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        return self.database.get_expiration(self.key)


@command(b"pttl", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class TimeToLiveMilliseconds(Command):
    """
    summary: Returns the expiration time in milliseconds of a key.
    complexity: O(1)
    since: 2.6.0
    function: pttlCommand
    reply_schema:
      oneOf:
      - description: TTL in milliseconds.
        type: integer
        minimum: 0
      - description: The key exists but has no associated expire.
        const: -1
      - description: The key does not exist.
        const: -2
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_time_to_live(self.key)
            if expiration is None:
                return -1
            return expiration
        except KeyError:
            return -2


@command(b"randomkey", {b"keyspace", b"read", b"slow"}, flags={b"readonly", b"touches_arbitrary_keys"})
class RandomKey(Command):
    """
    summary: Returns a random key name from the database.
    complexity: O(1)
    since: 1.0.0
    function: randomkeyCommand
    reply_schema:
      oneOf:
      - description: When the database is empty.
        type: 'null'
      - description: Random key in db.
        type: string
    """

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


@command(b"rename", {b"keyspace", b"slow"}, flags={b"write"})
class Rename(Command):
    """
    summary: Renames a key and overwrites the destination.
    complexity: O(1)
    since: 1.0.0
    function: renameCommand
    reply_schema:
      const: OK
    """

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


@command(b"renamenx", {b"keyspace"}, flags={b"fast", b"write"})
class RenameIfNotExists(Command):
    """
    summary: Renames a key only when the target key name doesn't exist.
    complexity: O(1)
    since: 1.0.0
    function: renamenxCommand
    reply_schema:
      oneOf:
      - description: Key was renamed to newkey.
        const: 1
      - description: New key already exists.
        const: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            raise ServerError(b"ERR no such key")
        if self.database.has_key(self.new_key):
            return 0

        self.database.rename_unsafely(self.key, self.new_key)

        return 1


@command(b"restore", {b"dangerous", b"keyspace", b"slow"}, flags={b"denyoom", b"write"})
class Restore(Command):
    """
    summary: Creates a key from the serialized representation of a value.
    complexity: >-
      O(1) to create the new key and additional O(N*M) to reconstruct the serialized value, where N is the number
      of objects composing the value and M their average size. For small string values the time complexity is thus
      O(1)+O(1*M) where M is small, so simply O(1). However for sorted set values the complexity is O(N*M*log(N))
      because inserting values into sorted sets is O(log(N)).
    since: 2.6.0
    function: restoreCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"W")
    ttl: int = positional_parameter()
    serialized_value: bytes = positional_parameter()
    replace: bool = flag_parameter(token=b"REPLACE")
    absolute_ttl: bool = flag_parameter(token=b"ABSTTL")
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


@command(b"scan", {b"keyspace", b"read", b"slow"}, flags={b"readonly", b"touches_arbitrary_keys"})
class Scan(Command):
    """
    summary: Iterates over the key names in the database.
    complexity: >-
      O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return
      back to 0. N is the number of elements inside the collection.
    since: 2.8.0
    function: scanCommand
    reply_schema:
      description: Cursor and scan response in array form.
      type: array
      minItems: 2
      maxItems: 2
      items:
      - description: Cursor.
        type: string
      - description: List of keys.
        type: array
        items:
          type: string
    """

    database: Database = dependency()
    cursor: int = positional_parameter()
    match: bytes | None = keyword_parameter(token=b"MATCH", default=None)
    count: int | None = keyword_parameter(token=b"COUNT", default=None)
    type: bytes | None = keyword_parameter(token=b"TYPE", default=None)

    def execute(self) -> ValueType:
        keys = list(self.database.keys())
        if self.match is not None:
            keys = fnmatch.filter(self.database.keys(), self.match)
        if self.type is not None:

            def type_filter(key: bytes) -> bool:
                value = self.database.get_value_or_none(key)
                if self.type == b"string":
                    return isinstance(value, bytes)
                if self.type == b"list":
                    return isinstance(value, list)
                if self.type == b"set":
                    return isinstance(value, set)
                if self.type == b"hash":
                    return isinstance(value, dict)
                if self.type == b"zset":
                    return isinstance(value, ScoredSortedSet)
                if self.type == b"stream":
                    return isinstance(value, Stream)
                return False

            keys = [key for key in keys if type_filter(key)]
        return [b"0", keys]


@command(b"sort", {b"dangerous", b"list", b"set", b"slow", b"sortedset"}, flags={b"denyoom", b"write"})
class Sort(Command):
    """
    summary: >-
      Sorts the elements in a list, a set, or a sorted set, optionally storing the result.
    complexity: >-
      O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements.
      When the elements are not sorted, complexity is O(N).
    since: 1.0.0
    function: sortCommand
    reply_schema:
      oneOf:
      - description: >-
          When the store option is specified the command returns the number of sorted elements in the destination
          list.
        type: integer
        minimum: 0
      - description: When not passing the store option the command returns a list of sorted elements.
        type: array
        items:
          oneOf:
          - type: string
          - description: GET option is specified, but no object was found.
            type: 'null'
    """

    database: Database = dependency()
    blocking_manager: BlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = flag_parameter(token=b"ALPHA")
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

        destination_values: list[bytes] = [
            (v if isinstance(v, bytes) else (str(v).encode() if isinstance(v, int) else b"")) for v in result_values
        ]
        self.database.set_key_value(
            KeyValue(self.destination, destination_values),
        )
        return len(result_values)

    async def after(self, in_multi: bool = False) -> None:
        if self.destination is None:
            return
        await self.blocking_manager.notify_safely(self.database, self.destination, in_multi=in_multi)


@command(b"sort_ro", {b"dangerous", b"list", b"read", b"set", b"slow", b"sortedset"}, flags={b"readonly"})
class SortReadOnly(Command):
    """
    summary: Returns the sorted elements of a list, a set, or a sorted set.
    complexity: >-
      O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements.
      When the elements are not sorted, complexity is O(N).
    since: 7.0.0
    function: sortroCommand
    reply_schema:
      description: A list of sorted elements.
      type: array
      items:
        oneOf:
        - type: string
        - description: GET option is specified, but no object was found.
          type: 'null'
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = flag_parameter(token=b"ALPHA")

    def _get_referenced_value(self, value: bytes, reference: bytes) -> int | bytes | None:
        reference_key, reference_field = reference, None
        if b"->" in reference_key and not reference_key.endswith(b"->"):
            reference_key, reference_field = reference.rsplit(b"->", 1)

        key_value = self.database.get_or_none(reference_key.replace(b"*", value, 1))
        if key_value is None:
            return None
        if reference_field:
            if isinstance(key_value.value, dict):
                dict_val = cast("dict[bytes, bytes | int]", key_value.value)
                return dict_val[reference_field]
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
        elif isinstance(key_value.value, list):
            values = list(cast("list[bytes]", key_value.value))
        else:
            values = list(cast("set[bytes]", key_value.value))

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


@command(b"touch", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class Touch(Command):
    """
    summary: >-
      Returns the number of existing keys out of those specified after updating the time they were last accessed.
    complexity: O(N) where N is the number of keys that will be touched.
    since: 3.2.1
    function: touchCommand
    reply_schema:
      description: The number of touched keys.
      type: integer
      minimum: 0
    """

    database: Database = dependency()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"ttl", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class TimeToLive(Command):
    """
    summary: Returns the expiration time in seconds of a key.
    complexity: O(1)
    since: 1.0.0
    function: ttlCommand
    reply_schema:
      oneOf:
      - description: TTL in seconds.
        type: integer
        minimum: 0
      - description: The key exists but has no associated expire.
        const: -1
      - description: The key does not exist.
        const: -2
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_time_to_live(self.key)
            if expiration is None:
                return -1
            return expiration // 1000
        except KeyError:
            return -2


@command(b"type", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class Type(Command):
    """
    summary: Determines the type of value stored at a key.
    complexity: O(1)
    since: 1.0.0
    function: typeCommand
    reply_schema:
      oneOf:
      - description: Key doesn't exist
        type: 'null'
      - description: Type of the key
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

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


@command(b"unlink", {b"keyspace"}, flags={b"fast", b"write"})
class Unlink(Command):
    """
    summary: Asynchronously deletes one or more keys.
    complexity: >-
      O(1) for each key removed regardless of its size. Then the command does O(N) work in a different thread in
      order to reclaim memory, where N is the number of allocations the deleted objects where composed of.
    since: 4.0.0
    function: unlinkCommand
    reply_schema:
      description: The number of keys that were unlinked.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    information: Information = dependency()

    blocking_manager: StreamBlockingManager = dependency()
    keys: list[bytes] = positional_parameter()

    _stream_keys: list[bytes] = field(default_factory=list, init=False)

    def execute(self) -> ValueType:
        count = 0
        for key in self.keys:
            value = self.database.pop(key, None)
            if value is not None:
                count += 1
                self.database.notify(NotificationType.GENERIC, b"del", key)
                if isinstance(value.value, Stream):
                    self._stream_keys.append(key)
                    if value.value.consumer_groups:
                        self.information.lazyfreed_objects += 1
                else:
                    self.information.lazyfreed_objects += 1

        return count

    async def after(self, in_multi: bool = False) -> None:
        for key in self._stream_keys:
            await self.blocking_manager.notify_deleted(key, in_multi=in_multi)


@command(b"wait", {b"blocking", b"connection", b"slow"}, flags={b"blocking"})
class Wait(Command):
    """
    summary: >-
      Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed.
    complexity: O(1)
    since: 3.0.0
    function: waitCommand
    reply_schema:
      type: integer
      description: >-
        The number of replicas reached by all the writes performed in the context of the current connection.
      minimum: 0
    """

    database: Database = dependency()
    numreplicas: int = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        return 0


@command(b"waitaof", {b"blocking", b"connection", b"slow"}, flags={b"blocking"})
class WaitAOF(Command):
    """
    summary: >-
      Blocks until all of the preceding write commands sent by the connection are written to the append-only file
      of the primary and/or replicas.
    complexity: O(1)
    since: 7.2.0
    function: waitaofCommand
    reply_schema:
      type: array
      description: Number of local and remote AOF files in sync.
      minItems: 2
      maxItems: 2
      items:
      - description: Number of local AOF files.
        type: integer
        minimum: 0
      - description: Number of replica AOF files.
        type: number
        minimum: 0
    """

    database: Database = dependency()
    numlocal: int = positional_parameter()
    numreplicas: int = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        return [0, 0]
