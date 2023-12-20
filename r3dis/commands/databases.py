import fnmatch
from dataclasses import dataclass

from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.databases import Database
from r3dis.resp import RESP_OK


@dataclass
class DatabaseCommand(Command):
    database: Database = redis_command_dependency()

    def execute(self):
        raise NotImplementedError()


@RedisCommandsRouter.command(b"echo", [b"fast", b"connection"])
class Echo(Command):
    message: bytes = redis_positional_parameter()

    def execute(self):
        return self.message


@RedisCommandsRouter.command(b"ping", [b"fast", b"connection"])
class Ping(Command):
    message: bytes = redis_positional_parameter(default=None)

    def execute(self):
        if self.message:
            return self.message
        return b"PONG"


@RedisCommandsRouter.command(b"flushdb", [b"keyspace", b"write", b"slow", b"dangerous"])
class FlushDatabase(DatabaseCommand):
    def execute(self):
        self.database.clear()
        return RESP_OK


@RedisCommandsRouter.command(b"get", [b"read", b"string", b"fast"])
class Get(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        if s := self.database.get_string_or_none(self.key):
            return s.bytes_value
        return None


@RedisCommandsRouter.command(b"set", [b"write", b"string", b"slow"])
class Set(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    value: bytes = redis_positional_parameter()

    def execute(self):
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(self.value)
        return RESP_OK


@RedisCommandsRouter.command(b"del", [b"keyspace", b"write", b"slow"])
class Delete(DatabaseCommand):
    keys: list[bytes] = redis_positional_parameter()

    def execute(self):
        return len([1 for _ in filter(None, [self.database.pop(key, None) for key in self.keys])])


@RedisCommandsRouter.command(b"keys", [b"keyspace", b"read", b"slow", b"dangerous"])
class Keys(DatabaseCommand):
    pattern: bytes = redis_positional_parameter()

    def execute(self):
        return list(fnmatch.filter(self.database.keys(), self.pattern))


@RedisCommandsRouter.command(b"dbsize", [b"keyspace", b"read", b"fast"])
class DatabaseSize(DatabaseCommand):
    def execute(self):
        return len(self.database.keys())


@RedisCommandsRouter.command(b"append", [b"write", b"string", b"fast"])
class Append(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    value: bytes = redis_positional_parameter()

    def execute(self):
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(s.bytes_value + self.value)
        return len(s)
