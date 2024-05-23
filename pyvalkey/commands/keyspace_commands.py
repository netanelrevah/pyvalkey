from pyvalkey.commands.context import ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.databases import DatabaseCommand
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter, server_keyword_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import RESP_OK, ValueType


@ServerCommandsRouter.command(b"flushdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushDatabase(DatabaseCommand):
    def execute(self) -> ValueType:
        self.database.clear()
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
    replace: bool = server_keyword_parameter(flag=b"REPLACE")

    def execute(self) -> ValueType:
        source_key = self.database.get_string(self.source)
        destination_key = self.database.get_string_or_none(self.destination)

        if self.replace and destination_key:
            self.database.pop(self.destination)
            destination_key = self.database.get_or_create_string(self.destination)
        else:
            return False

        destination_key.update_with_bytes_value(source_key.bytes_value)
        return True
