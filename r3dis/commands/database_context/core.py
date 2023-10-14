import fnmatch
from dataclasses import dataclass
from typing import Callable

from r3dis.commands.core import Command, CommandParser
from r3dis.databases import Database
from r3dis.errors import RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK


@dataclass
class DatabaseCommand(Command):
    database: Database

    def execute(self):
        raise NotImplementedError()


@dataclass
class DatabaseBytesParametersCommandParser(CommandParser):
    database: Database

    command_creator: Callable[[Database, bytes, ...], DatabaseCommand]

    number_of_parameters: int = None

    def parse(self, parameters: list[bytes]) -> Command:
        if self.number_of_parameters and len(parameters) != self.number_of_parameters:
            raise RedisWrongNumberOfArguments()
        return self.command_creator(self.database, *parameters)


@dataclass
class DatabaseCommandParser(CommandParser):
    database: Database


@dataclass
class FlushDatabase(DatabaseCommand):
    def execute(self):
        self.database.clear()
        return RESP_OK


@dataclass
class Get(DatabaseCommand):
    key: bytes

    def execute(self):
        if s := self.database.get_string_or_none(self.key):
            return s.bytes_value
        return None


@dataclass
class Set(DatabaseCommand):
    key: bytes
    value: bytes

    def execute(self):
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(self.value)
        return RESP_OK


@dataclass
class Delete(DatabaseCommand):
    keys: list[bytes]

    def execute(self):
        return len([1 for _ in filter(None, [self.database.pop(key, None) for key in self.keys])])


@dataclass
class Keys(DatabaseCommand):
    pattern: bytes

    def execute(self):
        return list(fnmatch.filter(self.database.keys(), self.pattern))


@dataclass
class DatabaseSize(DatabaseCommand):
    def execute(self):
        return len(self.database.keys())


@dataclass
class Append(DatabaseCommand):
    key: bytes
    value: bytes

    def execute(self):
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(s.bytes_value + self.value)
        return len(s)
