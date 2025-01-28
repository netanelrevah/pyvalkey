import sys

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import RESP_OK, RespError, ValueType


@ServerCommandsRouter.command(b"flushdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushDatabase(DatabaseCommand):
    def execute(self) -> ValueType:
        self.database.data.clear()
        self.database.key_with_expiration.clear()
        return RESP_OK


@ServerCommandsRouter.command(b"flushall", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushAllDatabases(Command):
    server_context: ServerContext = server_command_dependency()

    def execute(self) -> ValueType:
        for database_number in self.server_context.databases.keys():
            self.server_context.databases[database_number] = Database()
        return RESP_OK


@ServerCommandsRouter.command(b"move", [b"keyspace", b"write", b"slow", b"dangerous"])
class Move(Command):
    key: bytes = positional_parameter()
    db: int = positional_parameter()

    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"randomkey", [b"keyspace", b"write", b"slow", b"dangerous"])
class RandomKey(Command):
    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"swapdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class SwapDb(Command):
    index1: int = positional_parameter()
    index2: int = positional_parameter()

    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"copy", [b"keyspace", b"write", b"slow"])
class Copy(Command):
    client_context: ClientContext = server_command_dependency()

    source: bytes = positional_parameter(key_mode=b"R")
    destination: bytes = positional_parameter(key_mode=b"W")
    replace: bool = keyword_parameter(flag=b"REPLACE", default=False)
    db: int | None = keyword_parameter(flag=b"DB", default=None)

    def execute(self) -> ValueType:
        source_key = self.client_context.database.get_string(self.source)

        database = self.client_context.server_context.databases[
            self.client_context.current_database if self.db is None else self.db
        ]

        destination_key = database.get_string_or_none(self.destination)

        if self.replace and destination_key is not None:
            database.pop(self.destination)
            destination_key = database.get_or_create_string(self.destination)
        else:
            return False

        destination_key.value = source_key.value
        return True


@ServerCommandsRouter.command(b"rename", [b"keyspace", b"write", b"slow"])
class Rename(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if self.key not in self.database.data:
            return RespError(b"no such key")

        value = self.database.pop(self.key)
        if value is None:
            raise Exception("should not happen")
        self.database.set_value(self.new_key, value)

        return RESP_OK


@ServerCommandsRouter.command(b"renamenx", [b"keyspace", b"write", b"slow"])
class RenameIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if self.key not in self.database.data:
            return RespError(b"no such key")
        if self.key in self.database.data:
            return 0

        value = self.database.pop(self.key)
        if value is None:
            raise Exception("should not happen")
        self.database.set_value(self.new_key, value)
        return 1


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

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None
        if isinstance(key_value.value, list):
            if self.configuration.list_max_listpack_size < 0:
                if sys.getsizeof(key_value.value) <= (abs(self.configuration.list_max_listpack_size) * (4 * 1024)):
                    return b"listpack"
                else:
                    return b"quicklist"
            elif len(key_value.value) <= self.configuration.list_max_listpack_size:
                return b"listpack"
            return b"quicklist"

        if isinstance(key_value.value, set):
            if len(key_value.value) <= self.configuration.set_max_intset_entries:
                return b"intset"
            return b"hashtable"

        return b"raw"
