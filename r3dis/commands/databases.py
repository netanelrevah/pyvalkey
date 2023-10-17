import fnmatch
from dataclasses import dataclass

from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.consts import Commands
from r3dis.databases import Database
from r3dis.resp import RESP_OK

database_commands_router = RedisCommandsRouter()


@dataclass
class DatabaseCommand(Command):
    database: Database = redis_command_dependency()

    def execute(self):
        raise NotImplementedError()


@database_commands_router.command(Commands.FlushDatabase)
class FlushDatabase(DatabaseCommand):
    def execute(self):
        self.database.clear()
        return RESP_OK


@database_commands_router.command(Commands.Get)
class Get(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        if s := self.database.get_string_or_none(self.key):
            return s.bytes_value
        return None


@database_commands_router.command(Commands.Set)
class Set(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    value: bytes = redis_positional_parameter()

    def execute(self):
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(self.value)
        return RESP_OK


@database_commands_router.command(Commands.Delete)
class Delete(DatabaseCommand):
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return len([1 for _ in filter(None, [self.database.pop(key, None) for key in self.keys])])


@database_commands_router.command(Commands.Keys)
class Keys(DatabaseCommand):
    pattern: bytes = redis_positional_parameter()

    def execute(self):
        return list(fnmatch.filter(self.database.keys(), self.pattern))


@database_commands_router.command(Commands.DatabaseSize)
class DatabaseSize(DatabaseCommand):
    def execute(self):
        return len(self.database.keys())


@database_commands_router.command(Commands.Append)
class Append(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    value: bytes = redis_positional_parameter()

    def execute(self):
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(s.bytes_value + self.value)
        return len(s)
