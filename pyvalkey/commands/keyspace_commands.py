import fnmatch
import random

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, ValkeySortedSet
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ValueType


@ServerCommandsRouter.command(b"flushdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushDatabase(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        if self.client_context.current_database in self.client_context.server_context.databases:
            self.client_context.server_context.databases.pop(self.client_context.current_database)
        return RESP_OK


@ServerCommandsRouter.command(b"flushall", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushAllDatabases(Command):
    server_context: ServerContext = server_command_dependency()

    def execute(self) -> ValueType:
        self.server_context.databases.clear()
        return RESP_OK


@ServerCommandsRouter.command(b"move", [b"keyspace", b"write", b"slow", b"dangerous"])
class Move(Command):
    database: Database = server_command_dependency()
    server_context: ServerContext = server_command_dependency()

    key: bytes = positional_parameter()
    db: int = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.pop(self.key)

        if key_value is None:
            return True

        database = self.server_context.databases[self.db]

        if self.key in database.data:
            return False

        database.data[self.key] = key_value

        return True


@ServerCommandsRouter.command(b"randomkey", [b"keyspace", b"write", b"slow", b"dangerous"])
class RandomKey(Command):
    database: Database = server_command_dependency()

    def execute(self) -> ValueType:
        if not self.database.data:
            return None
        return random.choice(list(self.database.data.keys()))


@ServerCommandsRouter.command(b"swapdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class SwapDb(Command):
    server_context: ServerContext = server_command_dependency()

    index1: int = positional_parameter(parse_error=b"ERR invalid first DB index")
    index2: int = positional_parameter(parse_error=b"ERR invalid second DB index")

    def execute(self) -> ValueType:
        if self.index1 == self.index2:
            return RESP_OK

        if self.index1 not in self.server_context.databases:
            raise ServerError(b"ERR DB index is out of range")

        if self.index2 not in self.server_context.databases:
            self.server_context.databases[self.index2] = self.server_context.databases.pop(self.index1)
            return RESP_OK

        self.server_context.databases[self.index1], self.server_context.databases[self.index2] = (
            self.server_context.databases[self.index2],
            self.server_context.databases[self.index1],
        )
        return RESP_OK


@ServerCommandsRouter.command(b"copy", [b"keyspace", b"write", b"slow"])
class Copy(Command):
    client_context: ClientContext = server_command_dependency()

    source: bytes = positional_parameter(key_mode=b"R")
    destination: bytes = positional_parameter(key_mode=b"W")
    replace: bool = keyword_parameter(flag=b"REPLACE", default=False)
    db: int | None = keyword_parameter(flag=b"DB", default=None)

    def execute(self) -> ValueType:
        source_key_value = self.client_context.database.get_or_none(self.source)

        if source_key_value is None:
            return False

        database = self.client_context.server_context.databases[
            self.client_context.current_database if self.db is None else self.db
        ]

        destination_key_value = database.get_or_none(self.destination)

        if destination_key_value is None:
            database.copy_from(source_key_value, self.destination)
            return True

        if not self.replace:
            return False

        database.pop(self.destination)
        database.copy_from(source_key_value, self.destination)
        return True


@ServerCommandsRouter.command(b"rename", [b"keyspace", b"write", b"slow"])
class Rename(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if self.key not in self.database.data:
            raise ServerError(b"ERR no such key")

        self.database.rename_unsafely(self.key, self.new_key)

        return RESP_OK


@ServerCommandsRouter.command(b"renamenx", [b"keyspace", b"write", b"slow"])
class RenameIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if self.key not in self.database.data:
            raise ServerError(b"ERR no such key")
        if self.new_key in self.database.data:
            return 0

        self.database.rename_unsafely(self.key, self.new_key)

        return 1


@ServerCommandsRouter.command(b"expire", [b"keyspace", b"write", b"fast"])
class Expire(DatabaseCommand):
    key: bytes = positional_parameter()
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration(self.key, self.seconds * 1000)


@ServerCommandsRouter.command(b"ttl", [b"read", b"keyspace", b"fast"])
class TimeToLive(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_expiration(self.key)
            if expiration is None:
                return -1
            return expiration // 1000
        except KeyError:
            return -2


@ServerCommandsRouter.command(b"pttl", [b"read", b"keyspace", b"fast"])
class TimeToLiveMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_expiration(self.key)
            if expiration is None:
                return -1
            return expiration
        except KeyError:
            return -2


@ServerCommandsRouter.command(b"encoding", [b"read", b"keyspace", b"slow"], parent_command=b"object")
class ObjectEncoding(DatabaseCommand):
    configuration: Configurations = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"R")

    @classmethod
    def approximate_list_size(cls, value: list[bytes]) -> int:
        return sum(len(item) for item in value)

    def is_set_intset(self, value: set[bytes]) -> bool:
        if len(value) > self.configuration.set_max_intset_entries:
            return False
        if any([not item.isdigit() for item in value]):
            return False
        return True

    def is_sorted_set_listpack(self, value: ValkeySortedSet) -> bool:
        if len(value) > self.configuration.zset_max_listpack_entries:
            return False
        if max(8 + len(m) for s, m in value.members) > self.configuration.zset_max_listpack_value:
            return False
        return True

    def is_dict_listpack(self, value: dict[bytes, bytes]) -> bool:
        if len(value) > self.configuration.hash_max_listpack_entries:
            return False
        if (
            max(4 + len(k) + (len(v) if isinstance(v, bytes) else len(str(v))) for k, v in value.items())
            > self.configuration.hash_max_listpack_value
        ):
            return False
        return True

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None
        if isinstance(key_value.value, list):
            if self.configuration.list_max_listpack_size < 0:
                if self.approximate_list_size(key_value.value) <= (
                    abs(self.configuration.list_max_listpack_size) * (4 * 1024)
                ):
                    return b"listpack"
                else:
                    return b"quicklist"
            elif len(key_value.value) <= self.configuration.list_max_listpack_size:
                return b"listpack"
            return b"quicklist"

        if isinstance(key_value.value, set):
            if self.is_set_intset(key_value.value):
                return b"intset"
            if len(key_value.value) <= self.configuration.set_max_listpack_entries:
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, ValkeySortedSet):
            if self.is_sorted_set_listpack(key_value.value):
                return b"listpack"
            return b"skiplist"

        if isinstance(key_value.value, dict):
            if self.is_dict_listpack(key_value.value):
                return b"listpack"
            return b"hashtable"

        return b"raw"


@ServerCommandsRouter.command(b"keys", [b"keyspace", b"read", b"slow", b"dangerous"])
class Keys(DatabaseCommand):
    MAXIMUM_NESTING = 1000

    pattern: bytes = positional_parameter()

    @classmethod
    def nesting(cls, pattern: bytes) -> int:
        return sum(1 for p in pattern.split(b"*")[1:] if p)

    def execute(self) -> ValueType:
        if self.nesting(self.pattern) > self.MAXIMUM_NESTING:
            return []
        return list(fnmatch.filter(self.database.data.keys(), self.pattern))


@ServerCommandsRouter.command(b"dump", [b"keyspace", b"read", b"slow"])
class Dump(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return b""


@ServerCommandsRouter.command(b"restore", [b"keyspace", b"write", b"slow", b"dangerous"])
class Restore(DatabaseCommand):
    key: bytes = positional_parameter()
    ttl: int = positional_parameter()
    serialized_value: bytes = positional_parameter()
    replace: bool = keyword_parameter(flag=b"REPLACE")
    absolute_ttl: bool = keyword_parameter(flag=b"ABSTTL")
    idle_time_seconds: bool = keyword_parameter(default=False, token=b"IDLETIME")
    frequency: bool = keyword_parameter(default=False, token=b"FREQ")

    def execute(self) -> ValueType:
        return RESP_OK
