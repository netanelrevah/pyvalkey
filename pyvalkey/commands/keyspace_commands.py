from pyvalkey.commands.context import ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.commands.strings_commands import DatabaseCommand
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import RESP_OK, ValueType


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


@ServerCommandsRouter.command(b"copy", [b"keyspace", b"write", b"slow"])
class Copy(DatabaseCommand):
    source: bytes = positional_parameter(key_mode=b"R")
    destination: bytes = positional_parameter(key_mode=b"W")
    replace: bool = keyword_parameter(flag=b"REPLACE")

    def execute(self) -> ValueType:
        source_key = self.database.get_string(self.source)
        destination_key = self.database.get_string_or_none(self.destination)

        if self.replace and destination_key:
            key_value = self.database.data.pop(self.destination)
            self.database.key_with_expiration.remove(key_value)
            destination_key = self.database.get_or_create_string(self.destination)
        else:
            return False

        destination_key.value = source_key.value
        return True
