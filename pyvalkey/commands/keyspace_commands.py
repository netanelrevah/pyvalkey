from pyvalkey.commands.context import ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.databases import DatabaseCommand
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import RESP_OK


@ServerCommandsRouter.command(b"flushdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushDatabase(DatabaseCommand):
    def execute(self):
        self.database.clear()
        return RESP_OK


@ServerCommandsRouter.command(b"flushall", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushDatabase(Command):
    server_context: ServerContext = server_command_dependency()

    def execute(self):
        for database_number in self.server_context.databases.keys():
            self.server_context.databases[database_number] = Database()
        return RESP_OK
